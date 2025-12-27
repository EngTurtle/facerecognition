<?php
/**
 * @copyright Copyright (c) 2017-2020 Matias De lellis <mati86dl@gmail.com>
 * @copyright Copyright (c) 2018, Branko Kokanovic <branko@kokanovic.org>
 *
 * @author Branko Kokanovic <branko@kokanovic.org>
 *
 * @license GNU AGPL version 3 or any later version
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */
namespace OCA\FaceRecognition\BackgroundJob\Tasks;

use OCP\IUser;

use OCP\Files\File;
use OCP\Files\Folder;
use OCP\Files\Node;
use OCP\Files\Cache\IFileAccess;

use OCA\FaceRecognition\BackgroundJob\FaceRecognitionBackgroundTask;
use OCA\FaceRecognition\BackgroundJob\FaceRecognitionContext;

use OCA\FaceRecognition\Db\Image;
use OCA\FaceRecognition\Db\ImageMapper;
use OCA\FaceRecognition\Db\FaceMapper;
use OCA\FaceRecognition\Db\PersonMapper;

use OCA\FaceRecognition\Service\FileService;
use OCA\FaceRecognition\Service\SettingsService;

/**
 * Task that, for each user, crawls for all images in database,
 * checks if they actually exist and removes them if they don't.
 * It should be executed rarely.
 */
class StaleImagesRemovalTask extends FaceRecognitionBackgroundTask {

	/** @var ImageMapper Image mapper */
	private $imageMapper;

	/** @var FaceMapper Face mapper */
	private $faceMapper;

	/** @var PersonMapper Person mapper */
	private $personMapper;

	/** @var FileService  File service*/
	private $fileService;

	/** @var SettingsService */
	private $settingsService;

	/** @var array Cache of validated parent paths to avoid repeated .nomedia checks */
	private $validatedParentPaths = [];

	/** @var int Counter for parent path cache hits (for logging) */
	private $cacheHits = 0;

	/** @var int Counter for parent path cache misses (for logging) */
	private $cacheMisses = 0;

	/**
	 * @param ImageMapper $imageMapper Image mapper
	 * @param FaceMapper $faceMapper Face mapper
	 * @param PersonMapper $personMapper Person mapper
	 * @param FileService $fileService File Service
	 * @param SettingsService $settingsService Settings Service
	 */
	public function __construct(ImageMapper     $imageMapper,
	                            FaceMapper      $faceMapper,
	                            PersonMapper    $personMapper,
	                            FileService     $fileService,
	                            SettingsService $settingsService)
	{
		parent::__construct();

		$this->imageMapper     = $imageMapper;
		$this->faceMapper      = $faceMapper;
		$this->personMapper    = $personMapper;
		$this->fileService     = $fileService;
		$this->settingsService = $settingsService;
	}

	/**
	 * @inheritdoc
	 */
	public function description() {
		return "Crawl for stale images (either missing in filesystem or under .nomedia) and remove them from DB";
	}

	/**
	 * @inheritdoc
	 */
	public function execute(FaceRecognitionContext $context) {
		$this->setContext($context);

		$staleRemovedImages = 0;

		$eligable_users = $this->context->getEligibleUsers();
		foreach($eligable_users as $user) {
			if (!$this->context->isRunningInSyncMode() &&
			    !$this->settingsService->getNeedRemoveStaleImages($user)) {
				// Completely skip this task for this user, seems that we already did full scan for him
				$this->logDebug(sprintf('Skipping stale images removal for user %s as there is no need for it', $user));
				continue;
			}

			// Since method below can take long time, it is generator itself
			$generator = $this->staleImagesRemovalForUser($user, $this->settingsService->getCurrentFaceModel());
			foreach ($generator as $_) {
				yield;
			}
			$staleRemovedImages += $generator->getReturn();

			$this->settingsService->setNeedRemoveStaleImages(false, $user);

			yield;
		}

		// NOTE: Dont remove, it is used within the Integration tests
		$this->context->propertyBag['StaleImagesRemovalTask_staleRemovedImages'] = $staleRemovedImages;
		return true;
	}

	/**
	 * Gets all images in database for a given user. For each image, check if it
	 * actually present in filesystem (and there is no .nomedia for it) and removes
	 * it from database if it is not present.
	 *
	 * @param string $userId ID of the user for which to remove stale images for
	 * @param int $model Used model
	 * @return \Generator|int Returns generator during yielding and finally returns int,
	 * which represent number of stale images removed
	 */
	private function staleImagesRemovalForUser(string $userId, int $model) {
		$this->fileService->setupFS($userId);

		// Get IFileAccess for bulk cache lookups
		$fileAccess = \OCP\Server::get(IFileAccess::class);
		$userFolder = $this->fileService->getUserFolder($userId);
		$storageId = $userFolder->getStorage()->getCache()->getNumericStorageId();

		$lastChecked = $this->settingsService->getLastStaleImageChecked($userId);
		$batchSize = 1000;
		$imagesRemoved = 0;
		$processed = 0;

		// Reset parent path cache and stats for each user
		$this->validatedParentPaths = [];
		$this->cacheHits = 0;
		$this->cacheMisses = 0;

		$this->logDebug(sprintf('Starting stale image removal for user %s from image ID %d', $userId, $lastChecked));
		yield;

		while (true) {
			// Fetch next batch from database (SQL-level filtering!)
			$batch = $this->imageMapper->findImagesAfter($userId, $model, $lastChecked, $batchSize);

			if (empty($batch)) {
				break; // No more images to process
			}

			$this->logDebug(sprintf('Processing batch of %d images for user %s', count($batch), $userId));

			// Extract file IDs for bulk lookup
			$fileIds = array_map(fn($img) => $img->getFile(), $batch);

			// PHASE 1: Bulk cache lookup - single query for up to 1000 files
			$cacheEntries = $fileAccess->getByFileIdsInStorage($fileIds, $storageId);
			$this->logDebug(sprintf('Bulk cache lookup: %d entries found from %d file IDs', count($cacheEntries), count($fileIds)));

			// PHASE 2: Process each image in the batch
			foreach ($batch as $image) {
				$fileId = $image->getFile();

				// Quick check: file missing from cache = definitely stale
				if (!isset($cacheEntries[$fileId])) {
					$this->deleteImage($image, $userId);
					$imagesRemoved++;
					$lastChecked = $image->id;
					continue;
				}

				// File exists in cache - get Node for mount/nomedia checks
				$nodes = $userFolder->getById($fileId);
				$node = count($nodes) > 0 ? $nodes[0] : null;

				$shouldDelete = false;

				if ($node === null) {
					// Shouldn't happen but defensive check
					$shouldDelete = true;
				} else if (!$this->fileService->isAllowedNode($node)) {
					// Mount type not allowed (shared/external/group)
					$shouldDelete = true;
				} else if ($this->isUnderNoDetectionCached($node)) {
					// Under .nomedia directory (with caching!)
					$shouldDelete = true;
				}

				if ($shouldDelete) {
					$this->deleteImage($image, $userId);
					$imagesRemoved++;
				}

				$lastChecked = $image->id;
				$processed++;

				// Yield every 200 images to allow other background tasks
				if ($processed % 200 === 0) {
					$this->logDebug(sprintf('Processed %d images for user %s (%d removed)', $processed, $userId, $imagesRemoved));
					yield;
				}
			}

			// Save progress after each batch
			$this->settingsService->setLastStaleImageChecked($lastChecked, $userId);
			yield;
		}

		// Reset checkpoint when complete
		$this->settingsService->setLastStaleImageChecked(0, $userId);

		$this->logInfo(sprintf('Completed stale image removal for user %s: processed %d images, removed %d stale images',
			$userId, $processed, $imagesRemoved));

		// Log cache efficiency stats
		$totalCacheChecks = $this->cacheHits + $this->cacheMisses;
		if ($totalCacheChecks > 0) {
			$cacheHitRate = ($this->cacheHits / $totalCacheChecks) * 100;
			$this->logDebug(sprintf('Parent path cache stats: %d hits, %d misses (%.1f%% hit rate)',
				$this->cacheHits, $this->cacheMisses, $cacheHitRate));
		}

		return $imagesRemoved;
	}

	private function deleteImage(Image $image, string $userId): void {
		$this->logInfo(sprintf('Removing stale image %d for user %s', $image->id, $userId));
		// note that invalidatePersons depends on existence of faces for a given image,
		// and we must invalidate before we delete faces!
		// TODO: this is same method as in Watcher, find where to unify them.
		$this->personMapper->invalidatePersons($image->id);
		$this->faceMapper->removeFromImage($image->id);
		$this->imageMapper->delete($image);
	}

	/**
	 * Check if node is under .nomedia directory with parent path caching
	 * Caches validated parent paths to avoid repeated tree walks
	 *
	 * @param Node $node The node to check
	 * @return bool True if under .nomedia directory
	 */
	private function isUnderNoDetectionCached(Node $node): bool {
		$parentPath = dirname($node->getPath());

		// Check if we've already validated this parent folder
		if (isset($this->validatedParentPaths[$parentPath])) {
			$this->cacheHits++;
			return false; // Parent was validated as NOT under .nomedia
		}

		// Perform the expensive tree walk check
		$this->cacheMisses++;
		$isUnderNoDetection = $this->fileService->isUnderNoDetection($node);

		if (!$isUnderNoDetection) {
			// Cache this parent as validated (not under .nomedia)
			$this->validatedParentPaths[$parentPath] = true;
		}

		return $isUnderNoDetection;
	}
}
