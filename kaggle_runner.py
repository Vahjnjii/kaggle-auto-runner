#!/usr/bin/env python3
"""
═══════════════════════════════════════════════════════════════════════════
    KAGGLE NOTEBOOK AUTO-RUNNER - RAILWAY.APP OPTIMIZED
═══════════════════════════════════════════════════════════════════════════

Automatically executes Kaggle notebook every 5 minutes.
Optimized for Railway.app deployment with detailed logging.

Author: Auto-generated for shreevathsbbhh
Notebook: shreevathsbbhh/new-15
Schedule: Every 5 minutes

═══════════════════════════════════════════════════════════════════════════
"""

import os
import sys
import json
import time
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import shutil

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

KAGGLE_USERNAME = "shreevathsbbhh"
KAGGLE_KEY = "9f167cdee8a045c97ca6a2f82c6701f9"
NOTEBOOK_SLUG = "shreevathsbbhh/new-15"
NOTEBOOK_NAME = "new-15"

# Execution interval in minutes
RUN_INTERVAL_MINUTES = 5

# ═══════════════════════════════════════════════════════════════════════════
# DEPENDENCY INSTALLER
# ═══════════════════════════════════════════════════════════════════════════

def ensure_dependencies():
    """Auto-install required packages if missing"""
    print("═" * 70)
    print("🔍 CHECKING DEPENDENCIES")
    print("═" * 70)
    
    required_packages = {
        'kaggle': 'kaggle==1.6.17',
        'schedule': 'schedule==1.2.0'
    }
    
    missing_packages = []
    
    for package, pip_name in required_packages.items():
        try:
            __import__(package)
            print(f"✅ {package:15} - Already installed")
        except ImportError:
            print(f"❌ {package:15} - Not found, will install")
            missing_packages.append(pip_name)
    
    if missing_packages:
        print(f"\n📦 Installing: {', '.join(missing_packages)}")
        try:
            subprocess.check_call(
                [sys.executable, "-m", "pip", "install", "--quiet", "--upgrade"] + missing_packages,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL
            )
            print("✅ All packages installed successfully")
        except subprocess.CalledProcessError as e:
            print(f"❌ Installation failed: {e}")
            sys.exit(1)
    else:
        print("\n✅ All required packages are installed")
    
    print("═" * 70 + "\n")

# Install dependencies before importing
ensure_dependencies()

# Now safe to import
import schedule

# ═══════════════════════════════════════════════════════════════════════════
# KAGGLE RUNNER CLASS
# ═══════════════════════════════════════════════════════════════════════════

class KaggleNotebookRunner:
    """Handles all Kaggle notebook execution operations"""
    
    def __init__(self):
        self.notebook_dir = Path("./notebook_temp")
        self.execution_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.start_time = datetime.utcnow()
        self.last_execution_time = None
    
    def log(self, message, symbol="ℹ️", level="INFO"):
        """Formatted logging with timestamp and symbol"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {symbol} {message}")
        sys.stdout.flush()  # Force immediate output for Railway logs
    
    def print_separator(self, char="═", length=70):
        """Print a separator line"""
        print(char * length)
        sys.stdout.flush()
    
    def setup_kaggle_credentials(self):
        """Setup Kaggle API credentials in user home directory"""
        try:
            kaggle_dir = Path.home() / ".kaggle"
            kaggle_dir.mkdir(exist_ok=True)
            
            credentials = {
                "username": KAGGLE_USERNAME,
                "key": KAGGLE_KEY
            }
            
            kaggle_json = kaggle_dir / "kaggle.json"
            with open(kaggle_json, 'w') as f:
                json.dump(credentials, f, indent=2)
            
            # Set proper permissions (Unix-like systems)
            try:
                kaggle_json.chmod(0o600)
            except Exception:
                pass  # Windows doesn't support chmod
            
            self.log("Kaggle credentials configured", "✅")
            return True
            
        except Exception as e:
            self.log(f"Failed to setup credentials: {str(e)}", "❌", "ERROR")
            return False
    
    def run_command(self, cmd, description, timeout=300):
        """Execute shell command with comprehensive error handling"""
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
            if result.stdout and result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        print(f"    │ {line}")
                sys.stdout.flush()
            
            # Check for errors
            if result.returncode != 0:
                error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                self.log(f"{description} failed", "❌", "ERROR")
                if error_msg:
                    print(f"    │ Error: {error_msg}")
                return False
            
            self.log(f"{description} completed successfully", "✅")
            return True
        
        except subprocess.TimeoutExpired:
            self.log(f"{description} timed out after {timeout}s", "⏱️", "WARNING")
            return False
        except Exception as e:
            self.log(f"{description} exception: {str(e)}", "❌", "ERROR")
            return False
    
    def pull_notebook(self):
        """Pull notebook from Kaggle with metadata preservation"""
        # Clean and create directory
        if self.notebook_dir.exists():
            shutil.rmtree(self.notebook_dir)
        self.notebook_dir.mkdir(exist_ok=True)
        
        # Pull notebook with metadata
        cmd = f"kaggle kernels pull {NOTEBOOK_SLUG} -p {self.notebook_dir} -m"
        if not self.run_command(cmd, "Pulling notebook from Kaggle"):
            return False
        
        # Verify downloaded files
        notebook_file = self.notebook_dir / f"{NOTEBOOK_NAME}.ipynb"
        metadata_file = self.notebook_dir / "kernel-metadata.json"
        
        if not notebook_file.exists():
            self.log("Notebook file not found after download", "❌", "ERROR")
            return False
        
        file_size = notebook_file.stat().st_size
        self.log(f"Notebook downloaded ({file_size:,} bytes)", "📥")
        
        if metadata_file.exists():
            self.log("Metadata file preserved (datasets will be intact)", "📋")
        else:
            self.log("No metadata file found (datasets might be lost)", "⚠️", "WARNING")
        
        return True
    
    def push_notebook(self):
        """Push notebook to Kaggle to trigger execution"""
        original_dir = os.getcwd()
        
        try:
            os.chdir(self.notebook_dir)
            self.log("Pushing notebook to trigger Kaggle execution", "📤")
            success = self.run_command("kaggle kernels push", "Pushing to Kaggle")
            os.chdir(original_dir)
            return success
            
        except Exception as e:
            os.chdir(original_dir)
            self.log(f"Push operation failed: {str(e)}", "❌", "ERROR")
            return False
    
    def verify_execution(self):
        """Check notebook execution status on Kaggle"""
        self.log("Waiting 20 seconds for Kaggle to process...", "⏳")
        time.sleep(20)
        
        # Check status (non-critical, don't fail if this errors)
        cmd = f"kaggle kernels status {NOTEBOOK_SLUG}"
        self.run_command(cmd, "Checking execution status")
        
        notebook_url = f"https://www.kaggle.com/code/{NOTEBOOK_SLUG}"
        self.log(f"Notebook URL: {notebook_url}", "🔗")
    
    def cleanup(self):
        """Clean up temporary files to save disk space"""
        try:
            if self.notebook_dir.exists():
                shutil.rmtree(self.notebook_dir)
                self.log("Temporary files cleaned up", "🧹")
        except Exception as e:
            self.log(f"Cleanup warning: {str(e)}", "⚠️", "WARNING")
    
    def execute(self):
        """Main execution flow - runs one complete cycle"""
        self.execution_count += 1
        self.last_execution_time = datetime.utcnow()
        
        # Header
        print("\n")
        self.print_separator("═")
        print(f"{'KAGGLE NOTEBOOK EXECUTION':^70}")
        print(f"{'Execution #' + str(self.execution_count):^70}")
        print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'):^70}")
        self.print_separator("═")
        print()
        
        success = False
        
        try:
            # Step 1: Setup credentials
            if not self.setup_kaggle_credentials():
                raise Exception("Credential setup failed")
            
            # Step 2: Pull notebook
            if not self.pull_notebook():
                raise Exception("Failed to pull notebook from Kaggle")
            
            # Step 3: Push to trigger execution
            if not self.push_notebook():
                raise Exception("Failed to push notebook to Kaggle")
            
            # Step 4: Verify execution started
            self.verify_execution()
            
            # Success!
            print()
            self.print_separator("═")
            self.log("EXECUTION COMPLETED SUCCESSFULLY", "✅")
            self.print_separator("═")
            print()
            
            self.success_count += 1
            success = True
            
        except KeyboardInterrupt:
            self.log("Execution interrupted by user", "⚠️", "WARNING")
            self.failure_count += 1
            raise  # Re-raise to stop the scheduler
            
        except Exception as e:
            self.log(f"Execution failed: {str(e)}", "❌", "ERROR")
            self.failure_count += 1
            success = False
            
        finally:
            self.cleanup()
        
        return success
    
    def get_runtime_stats(self):
        """Calculate and return runtime statistics"""
        uptime = datetime.utcnow() - self.start_time
        hours, remainder = divmod(uptime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        success_rate = (self.success_count / self.execution_count * 100) if self.execution_count > 0 else 0
        
        # Calculate next execution time
        if self.last_execution_time:
            next_exec = self.last_execution_time + timedelta(minutes=RUN_INTERVAL_MINUTES)
            time_until_next = next_exec - datetime.utcnow()
            if time_until_next.total_seconds() > 0:
                mins, secs = divmod(time_until_next.total_seconds(), 60)
                next_run_str = f"{int(mins)}m {int(secs)}s"
            else:
                next_run_str = "Now"
        else:
            next_run_str = "Unknown"
        
        return {
            'total_executions': self.execution_count,
            'successful': self.success_count,
            'failed': self.failure_count,
            'success_rate': f"{success_rate:.1f}%",
            'uptime': f"{int(hours)}h {int(minutes)}m {int(seconds)}s",
            'started': self.start_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'next_run': next_run_str
        }

# ═══════════════════════════════════════════════════════════════════════════
# SCHEDULER & MAIN APPLICATION
# ═══════════════════════════════════════════════════════════════════════════

# Global runner instance
runner = KaggleNotebookRunner()

def scheduled_job():
    """Wrapper function called by scheduler"""
    try:
        runner.execute()
        
        # Display statistics
        stats = runner.get_runtime_stats()
        print()
        print("═" * 70)
        print(f"{'📊 RUNTIME STATISTICS':^70}")
        print("═" * 70)
        print(f"  Total Executions:  {stats['total_executions']}")
        print(f"  Successful:        {stats['successful']}")
        print(f"  Failed:            {stats['failed']}")
        print(f"  Success Rate:      {stats['success_rate']}")
        print(f"  System Uptime:     {stats['uptime']}")
        print(f"  Next Execution:    {stats['next_run']}")
        print("═" * 70)
        print()
        
    except Exception as e:
        print(f"\n❌ Scheduler error: {e}\n")
        sys.stdout.flush()

def display_startup_banner():
    """Display application startup information"""
    print("\n")
    print("🚀" * 35)
    print(f"{'KAGGLE NOTEBOOK AUTO-RUNNER':^70}")
    print(f"{'Railway.app Deployment':^70}")
    print("🚀" * 35)
    print()
    print(f"  ⏰ Schedule:      Every {RUN_INTERVAL_MINUTES} minutes")
    print(f"  📍 Notebook:      {NOTEBOOK_SLUG}")
    print(f"  🕐 Started:       {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
    print(f"  🔗 Notebook URL:  https://www.kaggle.com/code/{NOTEBOOK_SLUG}")
    print()
    print("  💡 Cost Estimate:")
    daily_execs = (24 * 60) // RUN_INTERVAL_MINUTES
    monthly_execs = daily_execs * 30
    print(f"     - Executions per day:   {daily_execs}")
    print(f"     - Executions per month: {monthly_execs:,}")
    print(f"     - Estimated cost:       ~${monthly_execs * 3 * 0.000463:.2f}/month")
    print()
    print("  ⚠️  Note: Railway.app free tier is $5/month")
    print()
    print("═" * 70)
    print()

def main():
    """Main application entry point"""
    try:
        # Display startup information
        display_startup_banner()
        
        # Setup scheduler
        schedule.every(RUN_INTERVAL_MINUTES).minutes.do(scheduled_job)
        print(f"✅ Scheduler configured: Every {RUN_INTERVAL_MINUTES} minutes\n")
        
        # Run first execution immediately
        print("⚡ Running first execution immediately (not waiting for schedule)...\n")
        scheduled_job()
        
        # Start scheduler loop
        print()
        print("═" * 70)
        print("🔄 SCHEDULER IS NOW RUNNING")
        print(f"   Next execution in {RUN_INTERVAL_MINUTES} minutes")
        print("   Press Ctrl+C to stop (if running locally)")
        print("═" * 70)
        print()
        
        # Main scheduler loop
        while True:
            schedule.run_pending()
            time.sleep(30)  # Check every 30 seconds
            
    except KeyboardInterrupt:
        # Graceful shutdown
        print("\n")
        print("═" * 70)
        print("👋 SHUTDOWN INITIATED")
        print("═" * 70)
        
        stats = runner.get_runtime_stats()
        print(f"  Total Executions:  {stats['total_executions']}")
        print(f"  Successful:        {stats['successful']}")
        print(f"  Failed:            {stats['failed']}")
        print(f"  Success Rate:      {stats['success_rate']}")
        print(f"  Total Uptime:      {stats['uptime']}")
        print(f"  Started:           {stats['started']}")
        print("═" * 70)
        print("\n✅ Shutdown complete\n")
        sys.exit(0)
        
    except Exception as e:
        print(f"\n\n❌ FATAL ERROR: {e}\n")
        sys.exit(1)

# ═══════════════════════════════════════════════════════════════════════════
# APPLICATION ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    main()
