#!/usr/bin/env python3
"""
Simple DBT Model Counter

This script counts the total number of models built from run_results.json files.
Can process single files or multiple files from a folder.
"""

import json
import sys
import os
import glob
from datetime import datetime
from pathlib import Path
import argparse


def find_run_results_files(path: str) -> list:
    """Find all run_results.json files in the given path."""
    run_results_files = []
    
    # If path is a file
    if os.path.isfile(path):
        if path.endswith('.json'):
            run_results_files.append(path)
        else:
            print(f"Warning: {path} is not a JSON file")
        return run_results_files
    
    # If path is a directory
    if os.path.isdir(path):
        # Look for run_results.json files
        pattern1 = os.path.join(path, "run_results.json")
        pattern2 = os.path.join(path, "**/run_results.json")
        pattern3 = os.path.join(path, "run_results*.json")
        
        # Find files matching patterns
        files = []
        files.extend(glob.glob(pattern1))
        files.extend(glob.glob(pattern2, recursive=True))
        files.extend(glob.glob(pattern3))
        
        # Remove duplicates
        run_results_files = list(set(files))
        
        if not run_results_files:
            print(f"No run_results.json files found in {path}")
        
        return sorted(run_results_files)
    
    print(f"Error: {path} is not a valid file or directory")
    return []


def count_models(file_path: str) -> dict:
    """Count the total number of models built from run_results.json."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: File {file_path} not found.")
        return None
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in {file_path}: {e}")
        return None
    
    # Extract metadata
    metadata = data.get('metadata', {})
    results = data.get('results', [])
    
    # Count models only (exclude tests, seeds, snapshots, etc.)
    total_models = 0
    successful_models = 0
    failed_models = 0
    
    # Count execution times for performance stats
    execution_times = []
    
    for result in results:
        unique_id = result.get('unique_id', '')
        
        # Only count models (not tests, seeds, snapshots, etc.)
        if unique_id.startswith('model.'):
            total_models += 1
            status = result.get('status', 'unknown')
            
            if status == 'success':
                successful_models += 1
            else:
                failed_models += 1
            
            # Get execution time if available
            execution_time = result.get('execution_time', 0)
            if execution_time > 0:
                execution_times.append(execution_time)
    
    # Calculate stats
    success_rate = (successful_models / total_models * 100) if total_models > 0 else 0
    avg_execution_time = sum(execution_times) / len(execution_times) if execution_times else 0
    total_execution_time = sum(execution_times)
    
    # Get run timestamp
    generated_at = metadata.get('generated_at', 'unknown')
    run_date = 'unknown'
    if generated_at != 'unknown':
        try:
            run_timestamp = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
            run_date = run_timestamp.date().isoformat()
        except ValueError:
            run_date = 'unknown'
    
    return {
        'file_path': file_path,
        'run_date': run_date,
        'run_timestamp': generated_at,
        'dbt_version': metadata.get('dbt_version', 'unknown'),
        'invocation_id': metadata.get('invocation_id', 'unknown'),
        'total_models': total_models,
        'successful_models': successful_models,
        'failed_models': failed_models,
        'success_rate': round(success_rate, 2),
        'avg_execution_time': round(avg_execution_time, 3),
        'total_execution_time': round(total_execution_time, 3)
    }


def print_single_result(stats: dict):
    """Print results for a single file."""
    print(f"\nFile: {stats['file_path']}")
    print(f"Date: {stats['run_date']}")
    print(f"Models Built: {stats['total_models']}")
    print(f"Success Rate: {stats['success_rate']}%")
    if stats['total_execution_time'] > 0:
        print(f"Execution Time: {stats['total_execution_time']:.2f}s")


def print_summary_results(all_stats: list):
    """Print summary results for multiple files."""
    if not all_stats:
        print("No results to display.")
        return
    
    print("\n" + "=" * 80)
    print("DBT MODEL BUILD SUMMARY")
    print("=" * 80)
    
    # Summary table
    print(f"\n{'File':<30} {'Date':<12} {'Models':<7} {'Success':<8} {'Time(s)':<8}")
    print("-" * 80)
    
    total_models = 0
    total_successful = 0
    total_time = 0
    
    for stats in sorted(all_stats, key=lambda x: x['run_date'], reverse=True):
        file_name = os.path.basename(stats['file_path'])[:28]
        models = stats['total_models']
        success_rate = stats['success_rate']
        exec_time = stats['total_execution_time']
        
        print(f"{file_name:<30} {stats['run_date']:<12} {models:<7} "
              f"{success_rate:>6.1f}% {exec_time:>7.2f}")
        
        total_models += models
        total_successful += stats['successful_models']
        total_time += exec_time
    
    # Overall summary
    print("-" * 80)
    overall_success_rate = (total_successful / total_models * 100) if total_models > 0 else 0
    print(f"{'TOTAL':<30} {'All Dates':<12} {total_models:<7} "
          f"{overall_success_rate:>6.1f}% {total_time:>7.2f}")
    
    print(f"\nOverall Statistics:")
    print(f"  Total Runs Analyzed: {len(all_stats)}")
    print(f"  Total Models Built: {total_models}")
    print(f"  Average Models per Run: {total_models / len(all_stats):.1f}")
    print(f"  Overall Success Rate: {overall_success_rate:.1f}%")
    print(f"  Total Execution Time: {total_time:.2f} seconds")


def main():
    """Main function."""
    parser = argparse.ArgumentParser(
        description='Count dbt models built from run_results.json files',
        epilog="""
Examples:
  %(prog)s run_results.json                    # Single file
  %(prog)s ./dbt_logs/                         # Folder with run_results.json files
  %(prog)s . --recursive                       # Current dir and subdirs
  %(prog)s folder/ --json                      # Output as JSON
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('path', nargs='?', default='run_results.json',
                       help='Path to run_results.json file or folder containing them (default: run_results.json)')
    parser.add_argument('--json', action='store_true',
                       help='Output results as JSON')
    parser.add_argument('--recursive', '-r', action='store_true',
                       help='Search recursively in subdirectories')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show detailed processing information')
    
    args = parser.parse_args()
    
    # Find all run_results.json files
    files = find_run_results_files(args.path)
    
    if not files:
        print("No run_results.json files found.")
        sys.exit(1)
    
    if args.verbose:
        print(f"Found {len(files)} file(s) to process:")
        for f in files:
            print(f"  - {f}")
        print()
    
    # Process all files
    all_stats = []
    for file_path in files:
        if args.verbose:
            print(f"Processing: {file_path}")
        
        stats = count_models(file_path)
        if stats:  # Only add if successfully processed
            all_stats.append(stats)
        elif args.verbose:
            print(f"  Failed to process {file_path}")
    
    if not all_stats:
        print("No valid run_results.json files could be processed.")
        sys.exit(1)
    
    # Output results
    if args.json:
        print(json.dumps(all_stats, indent=2))
    else:
        if len(all_stats) == 1:
            # Single file - show detailed view
            stats = all_stats[0]
            print("\n" + "=" * 60)
            print("DBT MODEL BUILD COUNT")
            print("=" * 60)
            print(f"\nFile: {stats['file_path']}")
            print(f"Date: {stats['run_date']}")
            print(f"DBT Version: {stats['dbt_version']}")
            print(f"Timestamp: {stats['run_timestamp']}")
            
            print(f"\nModel Statistics:")
            print(f"  Total Models Built: {stats['total_models']}")
            print(f"  Successful: {stats['successful_models']}")
            print(f"  Failed: {stats['failed_models']}")
            print(f"  Success Rate: {stats['success_rate']}%")
            
            if stats['total_execution_time'] > 0:
                print(f"\nPerformance:")
                print(f"  Average Execution Time: {stats['avg_execution_time']} seconds")
                print(f"  Total Execution Time: {stats['total_execution_time']} seconds")
        else:
            # Multiple files - show summary
            print_summary_results(all_stats)


if __name__ == "__main__":
    main()
