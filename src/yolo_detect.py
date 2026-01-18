#!/usr/bin/env python3
"""
YOLO Object Detection Script for Image Classification

Scans images downloaded in Task 1, runs YOLOv8 detection, and categorizes images
based on detected objects (promotional, product_display, lifestyle, other).
"""

import os
import csv
import logging
from pathlib import Path
from typing import List, Dict, Any, Optional
from collections import Counter

from ultralytics import YOLO
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Directory paths
BASE_DIR = Path(__file__).parent.parent
IMAGES_DIR = BASE_DIR / 'data' / 'raw' / 'images'
OUTPUT_DIR = BASE_DIR / 'data' / 'processed'
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# YOLO model path
YOLO_MODEL = 'yolov8n.pt'  # Using nano model for efficiency

# COCO class indices for relevant objects
# person: 0, bottle: 39, cup: 41, knife: 43, bowl: 46, banana: 46, apple: 52, etc.
PERSON_CLASS = 0
BOTTLE_CLASS = 39
CUP_CLASS = 41
BOWL_CLASS = 46

# Threshold for classification confidence
CONFIDENCE_THRESHOLD = 0.25


def classify_image(detections: List[Dict[str, Any]]) -> tuple[str, Dict[str, float]]:
    """
    Classify image based on detected objects.
    
    Classification scheme:
    - promotional: Contains person + product (bottle/cup/bowl or similar containers)
    - product_display: Contains bottle/container, no person
    - lifestyle: Contains person, no product
    - other: Neither person nor product detected
    
    Args:
        detections: List of detection dictionaries with 'class', 'confidence'
        
    Returns:
        Tuple of (category, dict of class_confidences)
    """
    detected_classes = [d['class'] for d in detections]
    class_confidences = {d['class']: d['confidence'] for d in detections}
    
    has_person = PERSON_CLASS in detected_classes
    has_product = any(cls in detected_classes for cls in [BOTTLE_CLASS, CUP_CLASS, BOWL_CLASS])
    
    if has_person and has_product:
        category = 'promotional'
    elif has_product and not has_person:
        category = 'product_display'
    elif has_person and not has_product:
        category = 'lifestyle'
    else:
        category = 'other'
    
    return category, class_confidences


def extract_message_id_from_path(image_path: Path) -> Optional[int]:
    """
    Extract message_id from image filename.
    
    Images are named as: {message_id}.jpg
    """
    try:
        return int(image_path.stem)
    except ValueError:
        logger.warning(f"Could not extract message_id from {image_path}")
        return None


def extract_channel_from_path(image_path: Path) -> Optional[str]:
    """
    Extract channel name from image path.
    
    Path format: data/raw/images/{channel_name}/{message_id}.jpg
    """
    try:
        # Get parent directory name (channel name)
        return image_path.parent.name
    except Exception as e:
        logger.warning(f"Could not extract channel from {image_path}: {e}")
        return None


def process_image(model: YOLO, image_path: Path) -> Dict[str, Any]:
    """
    Process a single image with YOLO model.
    
    Args:
        model: YOLO model instance
        image_path: Path to image file
        
    Returns:
        Dictionary with detection results
    """
    message_id = extract_message_id_from_path(image_path)
    channel_name = extract_channel_from_path(image_path)
    
    if message_id is None or channel_name is None:
        logger.warning(f"Skipping {image_path} - could not extract metadata")
        return None
    
    try:
        # Run YOLO detection
        results = model(str(image_path), conf=CONFIDENCE_THRESHOLD, verbose=False)
        
        # Extract detections
        detections = []
        detected_classes = []
        
        if results and len(results) > 0:
            result = results[0]
            if result.boxes is not None:
                boxes = result.boxes
                for i in range(len(boxes)):
                    cls_id = int(boxes.cls[i])
                    confidence = float(boxes.conf[i])
                    
                    detections.append({
                        'class': cls_id,
                        'class_name': result.names[cls_id],
                        'confidence': confidence
                    })
                    detected_classes.append(cls_id)
        
        # Classify image
        category, class_confidences = classify_image(detections)
        
        # Get top detected class
        top_detection = max(detections, key=lambda x: x['confidence']) if detections else None
        top_class = top_detection['class_name'] if top_detection else None
        top_confidence = top_detection['confidence'] if top_detection else 0.0
        
        result = {
            'message_id': message_id,
            'channel_name': channel_name,
            'image_path': str(image_path.relative_to(BASE_DIR)),
            'detected_objects_count': len(detections),
            'detected_classes': ','.join([str(c) for c in detected_classes]),
            'top_detected_class': top_class,
            'top_confidence': top_confidence,
            'image_category': category,
            'all_confidences': str(class_confidences)
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Error processing image {image_path}: {e}")
        return None


def find_all_images(images_dir: Path) -> List[Path]:
    """
    Find all image files in the images directory.
    
    Args:
        images_dir: Root directory containing channel subdirectories
        
    Returns:
        List of image file paths
    """
    image_extensions = {'.jpg', '.jpeg', '.png', '.JPG', '.JPEG', '.PNG'}
    images = []
    
    if not images_dir.exists():
        logger.warning(f"Images directory does not exist: {images_dir}")
        return images
    
    for channel_dir in images_dir.iterdir():
        if channel_dir.is_dir():
            for image_file in channel_dir.iterdir():
                if image_file.suffix in image_extensions:
                    images.append(image_file)
    
    logger.info(f"Found {len(images)} images to process")
    return images


def save_results_to_csv(results: List[Dict[str, Any]], output_file: Path):
    """
    Save detection results to CSV file.
    
    Args:
        results: List of detection result dictionaries
        output_file: Path to output CSV file
    """
    if not results:
        logger.warning("No results to save")
        return
    
    fieldnames = [
        'message_id',
        'channel_name',
        'image_path',
        'detected_objects_count',
        'detected_classes',
        'top_detected_class',
        'top_confidence',
        'image_category',
        'all_confidences'
    ]
    
    try:
        with open(output_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)
        
        logger.info(f"Saved {len(results)} detection results to {output_file}")
    except Exception as e:
        logger.error(f"Error saving results to CSV: {e}")
        raise


def main():
    """Main function to run YOLO detection on all images."""
    logger.info("Starting YOLO object detection pipeline")
    
    # Load YOLO model
    logger.info(f"Loading YOLO model: {YOLO_MODEL}")
    try:
        model = YOLO(YOLO_MODEL)
        logger.info("Model loaded successfully")
    except Exception as e:
        logger.error(f"Failed to load YOLO model: {e}")
        return
    
    # Find all images
    images = find_all_images(IMAGES_DIR)
    
    if not images:
        logger.warning("No images found to process")
        return
    
    # Process images
    results = []
    processed_count = 0
    error_count = 0
    
    logger.info(f"Processing {len(images)} images...")
    
    for i, image_path in enumerate(images, 1):
        if i % 100 == 0:
            logger.info(f"Processed {i}/{len(images)} images...")
        
        result = process_image(model, image_path)
        
        if result:
            results.append(result)
            processed_count += 1
        else:
            error_count += 1
    
    logger.info(f"Processing complete: {processed_count} successful, {error_count} errors")
    
    # Save results to CSV
    output_file = OUTPUT_DIR / 'yolo_detections.csv'
    save_results_to_csv(results, output_file)
    
    # Print summary statistics
    if results:
        categories = [r['image_category'] for r in results]
        category_counts = Counter(categories)
        
        logger.info("\n" + "="*50)
        logger.info("Detection Summary:")
        logger.info("="*50)
        logger.info(f"Total images processed: {len(results)}")
        logger.info("\nCategory distribution:")
        for category, count in category_counts.most_common():
            percentage = (count / len(results)) * 100
            logger.info(f"  {category}: {count} ({percentage:.1f}%)")
        
        # Channel statistics
        channels = [r['channel_name'] for r in results]
        channel_counts = Counter(channels)
        logger.info("\nImages by channel:")
        for channel, count in channel_counts.most_common():
            logger.info(f"  {channel}: {count}")
        
        logger.info(f"\nResults saved to: {output_file}")
        logger.info("="*50)


if __name__ == '__main__':
    main()
