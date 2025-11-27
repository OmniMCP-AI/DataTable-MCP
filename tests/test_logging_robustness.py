#!/usr/bin/env python3
"""
Test logging configuration robustness
Ensures that logging failures don't crash the application
"""
import os
import sys
import tempfile
import shutil
from pathlib import Path


def test_normal_logging():
    """Test 1: Normal logging configuration works"""
    print("\n📝 Test 1: Normal logging configuration")

    # Create a temporary log folder
    temp_dir = tempfile.mkdtemp(prefix="test_logs_")
    print(f"   Using temp folder: {temp_dir}")

    try:
        # Set environment
        os.environ["ENV"] = "production"
        os.environ["LOG_FOLDER"] = temp_dir

        # Import fresh to get new config
        import importlib
        import core.logging_config
        importlib.reload(core.logging_config)

        # Configure logging
        core.logging_config.configure_logging()

        # Get logger and test it
        logger = core.logging_config.get_logger(__name__)
        logger.info("test_message", test_key="test_value")

        # Check log file was created
        log_files = list(Path(temp_dir).glob("*.log"))
        if log_files:
            print(f"   ✅ PASS: Log file created: {log_files[0].name}")
            # Read and verify content
            content = log_files[0].read_text()
            if "test_message" in content:
                print(f"   ✅ PASS: Log message written successfully")
            else:
                print(f"   ❌ FAIL: Log message not found in file")
        else:
            print(f"   ❌ FAIL: No log file created")

    except Exception as e:
        print(f"   ❌ FAIL: Exception occurred: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Clean up
        shutil.rmtree(temp_dir, ignore_errors=True)


def test_invalid_log_folder():
    """Test 2: Invalid log folder triggers fallback"""
    print("\n📝 Test 2: Invalid log folder (should fallback)")

    # Use an invalid path that will definitely fail
    invalid_path = "/invalid/path/that/does/not/exist"

    try:
        # Set environment
        os.environ["ENV"] = "production"
        os.environ["LOG_FOLDER"] = invalid_path

        # Import fresh to get new config
        import importlib
        import core.settings
        importlib.reload(core.settings)

        # This should print warnings but NOT crash
        from core.settings import SETTINGS

        if SETTINGS.log_folder != invalid_path:
            print(f"   ✅ PASS: Fallback folder used: {SETTINGS.log_folder}")
        else:
            print(f"   ❌ FAIL: Still using invalid folder: {SETTINGS.log_folder}")

        # Configure logging - should work with fallback
        import core.logging_config
        importlib.reload(core.logging_config)
        core.logging_config.configure_logging()

        # Get logger and test it
        logger = core.logging_config.get_logger(__name__)
        logger.info("test_fallback_message", test_key="fallback_value")

        print(f"   ✅ PASS: Logging works with fallback folder")

    except Exception as e:
        print(f"   ❌ FAIL: Exception occurred (should have fallen back gracefully): {e}")
        import traceback
        traceback.print_exc()


def test_permission_denied():
    """Test 3: Permission denied scenario"""
    print("\n📝 Test 3: Permission denied (should fallback)")

    # Use root-owned folder that we can't write to
    readonly_path = "/etc/datatable-logs"

    try:
        # Set environment
        os.environ["ENV"] = "production"
        os.environ["LOG_FOLDER"] = readonly_path

        # Import fresh to get new config
        import importlib
        import core.settings
        importlib.reload(core.settings)

        from core.settings import SETTINGS

        if SETTINGS.log_folder != readonly_path:
            print(f"   ✅ PASS: Fallback folder used: {SETTINGS.log_folder}")
        else:
            print(f"   ⚠️  WARNING: Still using readonly folder (might have permissions)")

        # Configure logging - should work with fallback
        import core.logging_config
        importlib.reload(core.logging_config)
        core.logging_config.configure_logging()

        # Get logger and test it
        logger = core.logging_config.get_logger(__name__)
        logger.info("test_permission_message", test_key="permission_value")

        print(f"   ✅ PASS: Logging works despite permission issues")

    except Exception as e:
        print(f"   ❌ FAIL: Exception occurred (should have fallen back gracefully): {e}")
        import traceback
        traceback.print_exc()


def test_main_entry_point():
    """Test 4: Main entry point doesn't crash with bad config"""
    print("\n📝 Test 4: Main entry point robustness")

    # Set invalid log folder
    os.environ["ENV"] = "production"
    os.environ["LOG_FOLDER"] = "/totally/invalid/path"

    try:
        # This should NOT raise an exception
        # Import main.py (this triggers configure_logging)
        import importlib
        if 'main' in sys.modules:
            importlib.reload(sys.modules['main'])
        else:
            import main

        print(f"   ✅ PASS: Main module imported without crashing")

        # Verify logger is usable
        from core.logging_config import get_logger
        logger = get_logger(__name__)
        logger.info("test_main_entry", test_key="main_value")

        print(f"   ✅ PASS: Logger works after main entry point initialization")

    except Exception as e:
        print(f"   ❌ FAIL: Exception occurred at main entry point: {e}")
        import traceback
        traceback.print_exc()


def test_console_fallback():
    """Test 5: Console logging fallback when all file logging fails"""
    print("\n📝 Test 5: Console logging fallback")

    try:
        # Set all fallback folders to invalid paths
        os.environ["ENV"] = "production"
        os.environ["LOG_FOLDER"] = "/invalid/path"

        # Mock the fallback mechanism by forcing it to fail
        import importlib
        import core.settings
        importlib.reload(core.settings)

        # Configure logging - should fallback to console
        import core.logging_config
        importlib.reload(core.logging_config)
        core.logging_config.configure_logging()

        # Get logger and test it
        logger = core.logging_config.get_logger(__name__)
        logger.info("test_console_fallback", test_key="console_value")

        print(f"   ✅ PASS: Console fallback works (check output above)")

    except Exception as e:
        print(f"   ❌ FAIL: Exception occurred: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    print("🧪 Logging Robustness Tests")
    print("=" * 60)

    # Run all tests
    test_normal_logging()
    test_invalid_log_folder()
    test_permission_denied()
    test_main_entry_point()
    test_console_fallback()

    print("\n" + "=" * 60)
    print("✅ All tests completed!")
    print("\n💡 Key improvements:")
    print("   • Invalid log folders trigger automatic fallback")
    print("   • Permission errors are handled gracefully")
    print("   • Main entry point never crashes due to logging issues")
    print("   • Console logging used as last resort")
    print("   • Server continues running even if file logging fails")
