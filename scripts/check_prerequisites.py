"""
Check and Install Prerequisites for Modeling Pipeline

================================================================================
PURPOSE
================================================================================
Checks if all prerequisites are met for running the modeling pipeline:
- Required Python packages
- Trained models
- Posted aggregates
- Versioned park hours table
- Entity index

Optionally installs missing packages.

================================================================================
USAGE
================================================================================
  python scripts/check_prerequisites.py
  python scripts/check_prerequisites.py --install-missing
  python scripts/check_prerequisites.py --output-base "D:\\Path"
"""

from __future__ import annotations

import argparse
import importlib
import subprocess
import sys
from pathlib import Path
from typing import Optional

# Add src to path
if str(Path(__file__).parent.parent / "src") not in sys.path:
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.paths import get_output_base


# =============================================================================
# PACKAGE CHECKS
# =============================================================================

REQUIRED_PACKAGES = {
    "pandas": "pandas>=2.2.0",
    "numpy": "numpy",
    "pyarrow": "pyarrow>=14.0.0",
    "xgboost": "xgboost>=2.0.0",
    "sklearn": "scikit-learn>=1.3.0",
    "boto3": "boto3>=1.34.0",
    "requests": "requests>=2.31.0",
}


def check_package(package_name: str) -> tuple[bool, Optional[str]]:
    """
    Check if a package is installed.
    
    Returns:
        (is_installed, error_message)
    """
    # Handle sklearn -> scikit-learn mapping
    import_name = package_name
    if package_name == "sklearn":
        import_name = "sklearn"
    
    try:
        importlib.import_module(import_name)
        return True, None
    except ImportError:
        return False, f"Package '{package_name}' not found"


def install_package(package_spec: str) -> bool:
    """Install a package using pip."""
    try:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", package_spec],
            check=True,
            capture_output=True,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  Error installing {package_spec}: {e.stderr.decode() if e.stderr else 'Unknown error'}")
        return False


# =============================================================================
# FILE/DATA CHECKS
# =============================================================================

def check_entity_index(output_base: Path) -> tuple[bool, str]:
    """Check if entity index exists."""
    index_db = output_base / "state" / "entity_index.sqlite"
    if index_db.exists():
        return True, f"Entity index found: {index_db}"
    return False, f"Entity index not found: {index_db} (run build_entity_index.py)"


def check_models(output_base: Path, min_models: int = 1) -> tuple[bool, str, int]:
    """Check if trained models exist."""
    models_dir = output_base / "models"
    if not models_dir.exists():
        return False, f"Models directory not found: {models_dir}", 0
    
    # Count entities with both model types
    entity_dirs = [d for d in models_dir.iterdir() if d.is_dir()]
    entities_with_models = 0
    
    for entity_dir in entity_dirs:
        has_with_posted = (entity_dir / "model_with_posted.json").exists()
        has_without_posted = (entity_dir / "model_without_posted.json").exists()
        
        if has_with_posted and has_without_posted:
            entities_with_models += 1
    
    if entities_with_models >= min_models:
        return True, f"Found {entities_with_models} entities with trained models", entities_with_models
    return False, f"Found only {entities_with_models} entities with trained models (need at least {min_models})", entities_with_models


def check_posted_aggregates(output_base: Path) -> tuple[bool, str]:
    """Check if posted aggregates exist."""
    aggregates_path = output_base / "aggregates" / "posted_aggregates.parquet"
    if aggregates_path.exists():
        return True, f"Posted aggregates found: {aggregates_path}"
    return False, f"Posted aggregates not found: {aggregates_path} (run build_posted_aggregates.py)"


def check_versioned_park_hours(output_base: Path) -> tuple[bool, str]:
    """Check if versioned park hours table exists."""
    versioned_path = output_base / "dimension_tables" / "dimparkhours_with_donor.csv"
    if versioned_path.exists():
        return True, f"Versioned park hours found: {versioned_path}"
    return False, f"Versioned park hours not found: {versioned_path} (optional - can use flat dimparkhours.csv)"


def check_encoding_mappings(output_base: Path) -> tuple[bool, str]:
    """Check if encoding mappings exist."""
    mappings_path = output_base / "state" / "encoding_mappings.json"
    if mappings_path.exists():
        return True, f"Encoding mappings found: {mappings_path}"
    return False, f"Encoding mappings not found: {mappings_path} (will be created during first encoding)"


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Check prerequisites for modeling pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    
    parser.add_argument(
        "--output-base",
        type=str,
        help="Pipeline output base directory (default: from config/config.json)",
    )
    
    parser.add_argument(
        "--install-missing",
        action="store_true",
        help="Install missing Python packages automatically",
    )
    
    parser.add_argument(
        "--min-models",
        type=int,
        default=1,
        help="Minimum number of entities with trained models required (default: 1)",
    )
    
    args = parser.parse_args()
    
    # Get output base
    if args.output_base:
        base = Path(args.output_base)
    else:
        base = get_output_base()
    
    print("=" * 80)
    print("Modeling Pipeline Prerequisites Check")
    print("=" * 80)
    print(f"Output base: {base}")
    print()
    
    all_checks_passed = True
    
    # Check Python packages
    print("1. Python Packages")
    print("-" * 80)
    
    missing_packages = []
    for package_name, package_spec in REQUIRED_PACKAGES.items():
        is_installed, error = check_package(package_name)
        if is_installed:
            print(f"  ✓ {package_name}")
        else:
            print(f"  ✗ {package_name} - {error}")
            missing_packages.append((package_name, package_spec))
            all_checks_passed = False
    
    if missing_packages:
        print()
        if args.install_missing:
            print("Installing missing packages...")
            for package_name, package_spec in missing_packages:
                print(f"  Installing {package_spec}...")
                if install_package(package_spec):
                    print(f"  ✓ Installed {package_name}")
                    all_checks_passed = True  # Re-check after install
                else:
                    print(f"  ✗ Failed to install {package_name}")
                    all_checks_passed = False
        else:
            print("To install missing packages, run with --install-missing")
    
    print()
    
    # Check entity index
    print("2. Entity Index")
    print("-" * 80)
    index_ok, index_msg = check_entity_index(base)
    if index_ok:
        print(f"  ✓ {index_msg}")
    else:
        print(f"  ✗ {index_msg}")
        all_checks_passed = False
    print()
    
    # Check models
    print("3. Trained Models")
    print("-" * 80)
    models_ok, models_msg, model_count = check_models(base, args.min_models)
    if models_ok:
        print(f"  ✓ {models_msg}")
    else:
        print(f"  ✗ {models_msg}")
        print(f"    Run: python scripts/train_entity_model.py --entity <ENTITY_CODE>")
        all_checks_passed = False
    print()
    
    # Check posted aggregates
    print("4. Posted Aggregates")
    print("-" * 80)
    aggregates_ok, aggregates_msg = check_posted_aggregates(base)
    if aggregates_ok:
        print(f"  ✓ {aggregates_msg}")
    else:
        print(f"  ✗ {aggregates_msg}")
        print(f"    Run: python scripts/build_posted_aggregates.py")
        all_checks_passed = False
    print()
    
    # Check versioned park hours (optional)
    print("5. Versioned Park Hours (Optional)")
    print("-" * 80)
    park_hours_ok, park_hours_msg = check_versioned_park_hours(base)
    if park_hours_ok:
        print(f"  ✓ {park_hours_msg}")
    else:
        print(f"  ⚠ {park_hours_msg}")
        print(f"    Optional - can use flat dimparkhours.csv as fallback")
        print(f"    To create: python src/migrate_park_hours_to_versioned.py")
        print(f"    Then: python src/build_park_hours_donor.py")
    print()
    
    # Check encoding mappings (optional)
    print("6. Encoding Mappings (Optional)")
    print("-" * 80)
    encoding_ok, encoding_msg = check_encoding_mappings(base)
    if encoding_ok:
        print(f"  ✓ {encoding_msg}")
    else:
        print(f"  ⚠ {encoding_msg}")
        print(f"    Will be created automatically during first encoding")
    print()
    
    # Summary
    print("=" * 80)
    if all_checks_passed:
        print("✓ All required prerequisites are met!")
        print()
        print("You can now run:")
        print("  python scripts/test_modeling_pipeline.py")
        print("  python scripts/generate_forecast.py")
        print("  python scripts/generate_backfill.py")
        print("  python scripts/calculate_wti.py")
        sys.exit(0)
    else:
        print("✗ Some prerequisites are missing")
        print()
        print("Required actions:")
        if missing_packages and not args.install_missing:
            print("  1. Install missing packages:")
            print(f"     python scripts/check_prerequisites.py --install-missing")
        if not index_ok:
            print("  2. Build entity index:")
            print(f"     python src/build_entity_index.py")
        if not models_ok:
            print("  3. Train at least one model:")
            print(f"     python scripts/train_entity_model.py --entity <ENTITY_CODE>")
        if not aggregates_ok:
            print("  4. Build posted aggregates:")
            print(f"     python scripts/build_posted_aggregates.py")
        print()
        print("Optional (but recommended):")
        if not park_hours_ok:
            print("  - Create versioned park hours table (see above)")
        sys.exit(1)


if __name__ == "__main__":
    main()
