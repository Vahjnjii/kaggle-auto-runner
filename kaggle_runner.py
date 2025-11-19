#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
    KAGGLE NOTEBOOK AUTO-RUNNER - COMPLETE STANDALONE SOLUTION
═══════════════════════════════════════════════════════════════════════════

This single file handles EVERYTHING:
- Auto-installs dependencies (kaggle, schedule)
- Schedules notebook execution every 10 minutes (configurable)
- Manages Kaggle credentials
- Pulls, pushes, and monitors notebook execution
- Error handling and logging
- Auto-cleanup

NO OTHER FILES NEEDED!

Deploy to Render.com:
1. Upload this file to GitHub
2. Connect to Render.com as Background Worker
3. Build Command: pip install kaggle schedule
4. Start Command: python kaggle_runner.py

═══════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime
from pathlib import Path
import shutil

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION - CHANGE THESE VALUES
# ═══════════════════════════════════════════════════════════════════════════

KAGGLE_USERNAME = "shreevathsbbhh"
KAGGLE_KEY = "9f167cdee8a045c97ca6a2f82c6701f9"
NOTEBOOK_SLUG = "shreevathsbbhh/new-15"
NOTEBOOK_NAME = "new-15"

# Schedule Settings
RUN_INTERVAL_MINUTES = 10  # Change to 720 for every 12 hours, 360 for 6 hours, 60 for 1 hour

# ═══════════════════════════════════════════════════════════════════════════
# AUTO-INSTALLER - Ensures all dependencies are available
# ═══════════════════════════════════════════════════════════════════════════

def ensure_dependencies():
    """Auto-install required packages if missing"""
    required_packages = {
        'kaggle': 'kaggle==1.6.17',
        'schedule': 'schedule==1.2.0'
    }
    
    missing_packages = []
    
    for package, pip_name in required_packages.items():
        try:
            __import__(package)
        except ImportError:
            missing_packages.append(pip_name)
    
    if missing_packages:
        print("📦 Installing missing dependencies...")
        print(f"   Packages: {', '.join(missing_packages)}")
        
        try:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", "--quiet"
            ] + missing_packages)
            print("✅ Dependencies installed successfully\n")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install dependencies: {e}")
            sys.exit(1)
    else:
        print("✅ All dependencies available\n")

# Install dependencies before importing
ensure_dependencies()

# Now safe to import
import schedule

# ═══════════════════════════════════════════════════════════════════════════
# KAGGLE RUNNER CLASS
# ═══════════════════════════════════════════════════════════════════════════

class KaggleNotebookRunner:
    """Complete notebook execution handler with scheduling"""
    
    def __init__(self):
        self.notebook_dir = Path("./notebook")
        self.execution_count = 0
        self.start_time = datetime.utcnow()
    
    def log(self, message, symbol="ℹ️"):
        """Formatted logging with timestamp"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp} UTC] {symbol} {message}")
        sys.stdout.flush()  # Force output (important for cloud logs)
    
    def setup_kaggle_credentials(self):
        """Setup Kaggle API credentials"""
        try:
            kaggle_dir = Path.home() / ".kaggle"
            kaggle_dir.mkdir(exist_ok=True)
            
            credentials = {
                "username": KAGGLE_USERNAME,
                "key": KAGGLE_KEY
            }
            
            kaggle_json = kaggle_dir / "kaggle.json"
            with open(kaggle_json, 'w') as f:
                json.dump(credentials, f)
            
            kaggle_json.chmod(0o600)
            self.log("Kaggle credentials configured", "✅")
            return True
        except Exception as e:
            self.log(f"Failed to setup credentials: {str(e)}", "❌")
            return False
    
    def run_command(self, cmd, description, timeout=300):
        """Execute shell command with error handling and output capture"""
        self.log(f"Starting: {description}", "🔄")
        
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            # Display command output
            if result.stdout:
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        print(f"    {line}")
                sys.stdout.flush()
            
            if result.returncode != 0:
                error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                self.log(f"{description} failed: {error_msg}", "❌")
                return False
            
            self.log(f"{description} completed", "✅")
            return True
        
        except subprocess.TimeoutExpired:
            self.log(f"{description} timed out after {timeout}s", "⏱️")
            return False
        except Exception as e:
            self.log(f"{description} error: {str(e)}", "❌")
            return False
    
    def pull_notebook(self):
        """Pull notebook from Kaggle with metadata"""
        # Create fresh directory
        if self.notebook_dir.exists():
            shutil.rmtree(self.notebook_dir)
        self.notebook_dir.mkdir(exist_ok=True)
        
        cmd = f"kaggle kernels pull {NOTEBOOK_SLUG} -p {self.notebook_dir} -m"
        if not self.run_command(cmd, "Pulling notebook from Kaggle"):
            return False
        
        # Verify files exist
        notebook_file = self.notebook_dir / f"{NOTEBOOK_NAME}.ipynb"
        metadata_file = self.notebook_dir / "kernel-metadata.json"
        
        if not notebook_file.exists():
            self.log("Notebook file not found after pull", "❌")
            return False
        
        if metadata_file.exists():
            self.log("Metadata preserved (datasets intact)", "📋")
        else:
            self.log("No metadata file (may lose dataset links)", "⚠️")
        
        return True
    
    def push_notebook(self):
        """Push notebook to Kaggle to trigger execution"""
        original_dir = os.getcwd()
        
        try:
            os.chdir(self.notebook_dir)
            success = self.run_command("kaggle kernels push", "Pushing to Kaggle")
            os.chdir(original_dir)
            return success
        except Exception as e:
            os.chdir(original_dir)
            self.log(f"Push failed: {str(e)}", "❌")
            return False
    
    def verify_execution(self):
        """Check notebook execution status"""
        self.log("Waiting 20s for Kaggle to process...", "⏳")
        time.sleep(20)
        
        # Check status (don't fail if this errors)
        self.run_command(
            f"kaggle kernels status {NOTEBOOK_SLUG}",
            "Checking execution status"
        )
        
        notebook_url = f"https://www.kaggle.com/code/{NOTEBOOK_SLUG}"
        self.log(f"View notebook: {notebook_url}", "🔗")
    
    def cleanup(self):
        """Clean up temporary files"""
        try:
            if self.notebook_dir.exists():
                shutil.rmtree(self.notebook_dir)
                self.log("Cleaned up temporary files", "🧹")
        except Exception as e:
            self.log(f"Cleanup warning: {str(e)}", "⚠️")
    
    def execute(self):
        """Main execution flow - runs one complete cycle"""
        self.execution_count += 1
        
        print("\n" + "="*70)
        print(f"{'KAGGLE NOTEBOOK EXECUTION':^70}")
        print(f"{'Run #' + str(self.execution_count):^70}")
        print("="*70 + "\n")
        
        success = False
        
        try:
            # Step 1: Setup credentials
            if not self.setup_kaggle_credentials():
                raise Exception("Credential setup failed")
            
            # Step 2: Pull notebook with metadata
            if not self.pull_notebook():
                raise Exception("Failed to pull notebook")
            
            # Step 3: Push to trigger execution
            if not self.push_notebook():
                raise Exception("Failed to push notebook")
            
            # Step 4: Verify execution started
            self.verify_execution()
            
            print("\n" + "="*70)
            self.log("EXECUTION COMPLETED SUCCESSFULLY", "✅")
            print("="*70 + "\n")
            
            success = True
            
        except KeyboardInterrupt:
            self.log("Interrupted by user", "⚠️")
            success = False
        except Exception as e:
            self.log(f"Execution failed: {str(e)}", "❌")
            success = False
        finally:
            self.cleanup()
        
        return success
    
    def get_runtime_stats(self):
        """Get runtime statistics"""
        uptime = datetime.utcnow() - self.start_time
        hours, remainder = divmod(uptime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        return {
            'executions': self.execution_count,
            'uptime': f"{int(hours)}h {int(minutes)}m {int(seconds)}s",
            'started': self.start_time.strftime('%Y-%m-%d %H:%M:%S UTC')
        }

# ═══════════════════════════════════════════════════════════════════════════
# SCHEDULER & MAIN ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

# Global runner instance
runner = KaggleNotebookRunner()

def scheduled_job():
    """Job wrapper for scheduler"""
    try:
        runner.execute()
        
        # Show stats after each execution
        stats = runner.get_runtime_stats()
        print(f"\n📊 Stats: {stats['executions']} runs | Uptime: {stats['uptime']}")
        print(f"⏰ Next run in {RUN_INTERVAL_MINUTES} minutes...\n")
        
    except Exception as e:
        print(f"\n❌ Scheduler error: {e}\n")

def main():
    """Main entry point with scheduler"""
    print("\n" + "🚀"*35)
    print(f"{'KAGGLE NOTEBOOK AUTO-RUNNER':^70}")
    print(f"{'Single-File Standalone Version':^70}")
    print("🚀"*35)
    print(f"\n⏰ Schedule: Every {RUN_INTERVAL_MINUTES} minutes")
    print(f"📍 Notebook: {NOTEBOOK_SLUG}")
    print(f"🕐 Started: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"🔗 Notebook URL: https://www.kaggle.com/code/{NOTEBOOK_SLUG}\n")
    print("="*70 + "\n")
    
    # Schedule the job
    schedule.every(RUN_INTERVAL_MINUTES).minutes.do(scheduled_job)
    
    # Run first execution immediately
    print("⚡ Running first execution immediately...\n")
    scheduled_job()
    
    # Keep running scheduled jobs
    print("\n" + "="*70)
    print("🔄 Scheduler is now running continuously...")
    print("   Press Ctrl+C to stop")
    print("="*70 + "\n")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(30)  # Check every 30 seconds
    except KeyboardInterrupt:
        print("\n\n" + "="*70)
        stats = runner.get_runtime_stats()
        print("👋 SCHEDULER STOPPED")
        print(f"   Total executions: {stats['executions']}")
        print(f"   Total uptime: {stats['uptime']}")
        print(f"   Started: {stats['started']}")
        print("="*70 + "\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n\n❌ Fatal error: {e}\n")
        sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()
