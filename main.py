import os
import sys
import subprocess
from datetime import datetime, timedelta
from pathlib import Path
import shutil
import time

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION - NOW USING ENVIRONMENT VARIABLES
# ═══════════════════════════════════════════════════════════════════════════

# Set environment variables for Kaggle authentication
# Railway will provide these from the Variables tab
os.environ['KAGGLE_USERNAME'] = os.getenv('KAGGLE_USERNAME', 'shreevathsbbhh')
os.environ['KAGGLE_KEY'] = os.getenv('KAGGLE_KEY', '9f167cdee8a045c97ca6a2f82c6701f9')
os.environ['KAGGLE_CONFIG_DIR'] = os.getenv('KAGGLE_CONFIG_DIR', '/tmp/.kaggle')

NOTEBOOK_SLUG = "shreevathsbbhh/new-15"
NOTEBOOK_NAME = "new-15"
RUN_INTERVAL_MINUTES = 5

# ═══════════════════════════════════════════════════════════════════════════
# DEPENDENCY INSTALLER
# ═══════════════════════════════════════════════════════════════════════════

def ensure_dependencies():
    """Install required packages without importing them first"""
    print("═" * 70)
    print("🔍 CHECKING DEPENDENCIES")
    print("═" * 70)
    
    required_packages = ['kaggle==1.6.17', 'schedule==1.2.0']
    
    print("📦 Installing packages...")
    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", "--quiet", "--upgrade"] + required_packages,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        print("✅ All packages installed successfully")
    except subprocess.CalledProcessError as e:
        print(f"❌ Installation failed: {e}")
        sys.exit(1)
    
    print("═" * 70 + "\n")

ensure_dependencies()

# NOW safe to import kaggle and schedule
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
    
    def log(self, message, symbol="ℹ️"):
        """Formatted logging with timestamp"""
        timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {symbol} {message}")
        sys.stdout.flush()
    
    def print_separator(self, char="═", length=70):
        """Print a separator line"""
        print(char * length)
        sys.stdout.flush()
    
    def verify_credentials(self):
        """Verify Kaggle credentials are set"""
        username = os.getenv('KAGGLE_USERNAME')
        key = os.getenv('KAGGLE_KEY')
        
        if not username or not key:
            self.log("ERROR: Kaggle credentials not set in environment variables", "❌")
            self.log("Please set KAGGLE_USERNAME and KAGGLE_KEY in Railway Variables tab", "⚠️")
            return False
        
        self.log(f"Kaggle credentials found (Username: {username})", "✅")
        return True
    
    def run_command(self, cmd, description, timeout=300):
        """Execute shell command with error handling"""
        self.log(f"Starting: {description}", "🔄")
        
        try:
            result = subprocess.run(
                cmd,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout
            )
            
            if result.stdout and result.stdout.strip():
                for line in result.stdout.strip().split('\n'):
                    if line.strip():
                        print(f"    │ {line}")
                sys.stdout.flush()
            
            if result.returncode != 0:
                error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                self.log(f"{description} failed", "❌")
                if error_msg:
                    print(f"    │ Error: {error_msg}")
                return False
            
            self.log(f"{description} completed successfully", "✅")
            return True
        
        except subprocess.TimeoutExpired:
            self.log(f"{description} timed out after {timeout}s", "⏱️")
            return False
        except Exception as e:
            self.log(f"{description} exception: {str(e)}", "❌")
            return False
    
    def pull_notebook(self):
        """Pull notebook from Kaggle with metadata"""
        if self.notebook_dir.exists():
            shutil.rmtree(self.notebook_dir)
        self.notebook_dir.mkdir(exist_ok=True)
        
        cmd = f"kaggle kernels pull {NOTEBOOK_SLUG} -p {self.notebook_dir} -m"
        if not self.run_command(cmd, "Pulling notebook from Kaggle"):
            return False
        
        notebook_file = self.notebook_dir / f"{NOTEBOOK_NAME}.ipynb"
        metadata_file = self.notebook_dir / "kernel-metadata.json"
        
        if not notebook_file.exists():
            self.log("Notebook file not found after download", "❌")
            return False
        
        file_size = notebook_file.stat().st_size
        self.log(f"Notebook downloaded ({file_size:,} bytes)", "📥")
        
        if metadata_file.exists():
            self.log("Metadata file preserved", "📋")
        else:
            self.log("No metadata file found", "⚠️")
        
        return True
    
    def push_notebook(self):
        """Push notebook to Kaggle to trigger execution"""
        original_dir = os.getcwd()
        
        try:
            os.chdir(self.notebook_dir)
            self.log("Pushing notebook to trigger execution", "📤")
            success = self.run_command("kaggle kernels push", "Pushing to Kaggle")
            os.chdir(original_dir)
            return success
        except Exception as e:
            os.chdir(original_dir)
            self.log(f"Push operation failed: {str(e)}", "❌")
            return False
    
    def verify_execution(self):
        """Check notebook execution status"""
        self.log("Waiting 20 seconds for Kaggle to process...", "⏳")
        time.sleep(20)
        
        cmd = f"kaggle kernels status {NOTEBOOK_SLUG}"
        self.run_command(cmd, "Checking execution status")
        
        notebook_url = f"https://www.kaggle.com/code/{NOTEBOOK_SLUG}"
        self.log(f"Notebook URL: {notebook_url}", "🔗")
    
    def cleanup(self):
        """Clean up temporary files"""
        try:
            if self.notebook_dir.exists():
                shutil.rmtree(self.notebook_dir)
                self.log("Temporary files cleaned up", "🧹")
        except Exception as e:
            self.log(f"Cleanup warning: {str(e)}", "⚠️")
    
    def execute(self):
        """Main execution flow"""
        self.execution_count += 1
        self.last_execution_time = datetime.utcnow()
        
        print("\n")
        self.print_separator("═")
        print(f"{'KAGGLE NOTEBOOK EXECUTION':^70}")
        print(f"{'Execution #' + str(self.execution_count):^70}")
        print(f"{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC'):^70}")
        self.print_separator("═")
        print()
        
        success = False
        
        try:
            if not self.verify_credentials():
                raise Exception("Credential verification failed")
            
            if not self.pull_notebook():
                raise Exception("Failed to pull notebook")
            
            if not self.push_notebook():
                raise Exception("Failed to push notebook")
            
            self.verify_execution()
            
            print()
            self.print_separator("═")
            self.log("EXECUTION COMPLETED SUCCESSFULLY", "✅")
            self.print_separator("═")
            print()
            
            self.success_count += 1
            success = True
            
        except KeyboardInterrupt:
            self.log("Execution interrupted", "⚠️")
            self.failure_count += 1
            raise
        except Exception as e:
            self.log(f"Execution failed: {str(e)}", "❌")
            self.failure_count += 1
            success = False
        finally:
            self.cleanup()
        
        return success
    
    def get_runtime_stats(self):
        """Get runtime statistics"""
        uptime = datetime.utcnow() - self.start_time
        hours, remainder = divmod(uptime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        success_rate = (self.success_count / self.execution_count * 100) if self.execution_count > 0 else 0
        
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
            'total': self.execution_count,
            'success': self.success_count,
            'failed': self.failure_count,
            'rate': f"{success_rate:.1f}%",
            'uptime': f"{int(hours)}h {int(minutes)}m {int(seconds)}s",
            'next': next_run_str
        }

# ═══════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════

runner = KaggleNotebookRunner()

def scheduled_job():
    """Wrapper for scheduled execution"""
    try:
        runner.execute()
        stats = runner.get_runtime_stats()
        
        print()
        print("═" * 70)
        print(f"{'📊 STATISTICS':^70}")
        print("═" * 70)
        print(f"  Executions: {stats['total']} (✅ {stats['success']} | ❌ {stats['failed']})")
        print(f"  Success Rate: {stats['rate']}")
        print(f"  Uptime: {stats['uptime']}")
        print(f"  Next Run: {stats['next']}")
        print("═" * 70)
        print()
    except Exception as e:
        print(f"\n❌ Scheduler error: {e}\n")
        sys.stdout.flush()

def main():
    """Main entry point"""
    try:
        print("\n🚀" * 35)
        print(f"{'KAGGLE NOTEBOOK AUTO-RUNNER':^70}")
        print(f"{'Railway.app - Environment Variable Auth':^70}")
        print("🚀" * 35)
        print(f"\n  ⏰ Schedule: Every {RUN_INTERVAL_MINUTES} minutes")
        print(f"  📍 Notebook: {NOTEBOOK_SLUG}")
        print(f"  🕐 Started: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print(f"  🔗 URL: https://www.kaggle.com/code/{NOTEBOOK_SLUG}\n")
        print("═" * 70 + "\n")
        
        schedule.every(RUN_INTERVAL_MINUTES).minutes.do(scheduled_job)
        print(f"✅ Scheduler configured\n")
        
        print("⚡ Running first execution immediately...\n")
        scheduled_job()
        
        print("\n" + "═" * 70)
        print("🔄 SCHEDULER RUNNING")
        print(f"   Next execution in {RUN_INTERVAL_MINUTES} minutes")
        print("═" * 70 + "\n")
        
        while True:
            schedule.run_pending()
            time.sleep(30)
            
    except KeyboardInterrupt:
        print("\n" + "═" * 70)
        print("👋 SHUTDOWN")
        stats = runner.get_runtime_stats()
        print(f"  Total: {stats['total']} | Success: {stats['success']} | Failed: {stats['failed']}")
        print(f"  Uptime: {stats['uptime']}")
        print("═" * 70 + "\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ FATAL: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
