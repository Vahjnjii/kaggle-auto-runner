import os
import sys
import subprocess
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
import shutil
import time

# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION - DUAL ACCOUNT SETUP
# ═══════════════════════════════════════════════════════════════════════════

# Source Account (where notebooks are pulled from)
SOURCE_ACCOUNT = {
    "username": "shreevathsbbhh",
    "key": "9f167cdee8a045c97ca6a2f82c6701f9"
}

# Destination Account (where notebooks are executed)
DEST_ACCOUNT = {
    "username": "distinct4exist",
    "key": "c2767798395ca8c007e931d6f9d42752"
}

# List of notebooks to execute
NOTEBOOKS = [
    {
        "source_slug": "shreevathsbbhh/new-19-1",
        "notebook_name": "new-19-1",
        "dest_slug": "distinct4exist/new-19-1"
    },
    {
        "source_slug": "shreevathsbbhh/new-19-2",
        "notebook_name": "new-19-2",
        "dest_slug": "distinct4exist/new-19-2"
    }
]

RUN_INTERVAL_MINUTES = 5

# ═══════════════════════════════════════════════════════════════════════════
# DEPENDENCY INSTALLER
# ═══════════════════════════════════════════════════════════════════════════

def ensure_dependencies():
    """Install required packages"""
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

# NOW safe to import
import schedule

# ═══════════════════════════════════════════════════════════════════════════
# CROSS-ACCOUNT KAGGLE RUNNER
# ═══════════════════════════════════════════════════════════════════════════

class CrossAccountKaggleRunner:
    """Handles notebook execution across different Kaggle accounts"""
    
    def __init__(self):
        self.temp_dir = Path("./notebooks_temp")
        self.kaggle_config_dir = Path.home() / ".kaggle"
        self.execution_count = 0
        self.success_count = 0
        self.failure_count = 0
        self.start_time = datetime.now(timezone.utc)
        self.last_execution_time = None
        self.notebook_stats = {nb['notebook_name']: {'success': 0, 'failed': 0} for nb in NOTEBOOKS}
    
    def log(self, message, symbol="ℹ️"):
        """Formatted logging with timestamp"""
        timestamp = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {symbol} {message}")
        sys.stdout.flush()
    
    def print_separator(self, char="═", length=70):
        """Print a separator line"""
        print(char * length)
        sys.stdout.flush()
    
    def setup_kaggle_credentials(self, account):
        """Set up Kaggle credentials for a specific account"""
        self.kaggle_config_dir.mkdir(exist_ok=True)
        kaggle_json = self.kaggle_config_dir / "kaggle.json"
        
        credentials = {
            "username": account["username"],
            "key": account["key"]
        }
        
        with open(kaggle_json, 'w') as f:
            json.dump(credentials, f)
        
        kaggle_json.chmod(0o600)
        self.log(f"Switched to account: {account['username']}", "🔑")
    
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
                for line in result.stdout.strip().split('\n')[:10]:
                    if line.strip():
                        print(f"    │ {line}")
                sys.stdout.flush()
            
            if result.returncode != 0:
                error_msg = result.stderr.strip() if result.stderr else "Unknown error"
                self.log(f"{description} failed", "❌")
                if error_msg:
                    for line in error_msg.split('\n')[:5]:
                        if line.strip():
                            print(f"    │ Error: {line}")
                return False
            
            self.log(f"{description} completed successfully", "✅")
            return True
        
        except subprocess.TimeoutExpired:
            self.log(f"{description} timed out after {timeout}s", "⏱️")
            return False
        except Exception as e:
            self.log(f"{description} exception: {str(e)}", "❌")
            return False
    
    def pull_notebook_from_source(self, notebook_config):
        """Pull notebook from source account"""
        notebook_dir = self.temp_dir / notebook_config['notebook_name']
        
        if notebook_dir.exists():
            shutil.rmtree(notebook_dir)
        notebook_dir.mkdir(parents=True, exist_ok=True)
        
        # Switch to source account
        self.setup_kaggle_credentials(SOURCE_ACCOUNT)
        
        # Pull notebook
        cmd = f"kaggle kernels pull {notebook_config['source_slug']} -p {notebook_dir} -m"
        if not self.run_command(cmd, f"Pulling {notebook_config['notebook_name']} from source"):
            return None
        
        notebook_file = notebook_dir / f"{notebook_config['notebook_name']}.ipynb"
        
        if not notebook_file.exists():
            self.log(f"Notebook file not found: {notebook_file}", "❌")
            return None
        
        file_size = notebook_file.stat().st_size
        self.log(f"Downloaded {notebook_config['notebook_name']} ({file_size:,} bytes)", "📥")
        
        return notebook_dir
    
    def update_metadata_for_dest_account(self, notebook_dir, notebook_config):
        """Update kernel metadata for destination account"""
        metadata_file = notebook_dir / "kernel-metadata.json"
        
        if metadata_file.exists():
            with open(metadata_file, 'r') as f:
                metadata = json.load(f)
            
            metadata['id'] = notebook_config['dest_slug']
            metadata['slug'] = notebook_config['notebook_name']
            metadata['enable_gpu'] = metadata.get('enable_gpu', False)
            metadata['enable_internet'] = metadata.get('enable_internet', True)
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            self.log("Metadata updated for destination account", "📝")
        else:
            metadata = {
                "id": notebook_config['dest_slug'],
                "slug": notebook_config['notebook_name'],
                "kernel_type": "notebook",
                "language": "python",
                "enable_gpu": False,
                "enable_internet": True
            }
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            self.log("Created new metadata for destination account", "📝")
    
    def push_to_dest_account(self, notebook_dir, notebook_config):
        """Push notebook to destination account to trigger execution"""
        original_dir = os.getcwd()
        
        try:
            self.setup_kaggle_credentials(DEST_ACCOUNT)
            
            os.chdir(notebook_dir)
            self.log(f"Pushing {notebook_config['notebook_name']} to destination account", "📤")
            success = self.run_command("kaggle kernels push", "Pushing to destination")
            os.chdir(original_dir)
            return success
        except Exception as e:
            os.chdir(original_dir)
            self.log(f"Push operation failed: {str(e)}", "❌")
            return False
    
    def verify_execution(self, notebook_config):
        """Check notebook execution status"""
        self.log("Waiting 15 seconds for Kaggle to process...", "⏳")
        time.sleep(15)
        
        self.setup_kaggle_credentials(DEST_ACCOUNT)
        
        cmd = f"kaggle kernels status {notebook_config['dest_slug']}"
        self.run_command(cmd, "Checking execution status")
        
        notebook_url = f"https://www.kaggle.com/code/{notebook_config['dest_slug']}"
        self.log(f"Notebook URL: {notebook_url}", "🔗")
    
    def execute_notebook(self, notebook_config):
        """Execute a single notebook (pull from source, push to dest)"""
        print()
        self.print_separator("─")
        print(f"  📓 NOTEBOOK: {notebook_config['notebook_name']}")
        self.print_separator("─")
        print()
        
        try:
            notebook_dir = self.pull_notebook_from_source(notebook_config)
            if not notebook_dir:
                raise Exception("Failed to pull notebook from source")
            
            self.update_metadata_for_dest_account(notebook_dir, notebook_config)
            
            if not self.push_to_dest_account(notebook_dir, notebook_config):
                raise Exception("Failed to push notebook to destination")
            
            self.verify_execution(notebook_config)
            
            self.log(f"✅ {notebook_config['notebook_name']} executed successfully", "✅")
            self.notebook_stats[notebook_config['notebook_name']]['success'] += 1
            return True
            
        except Exception as e:
            self.log(f"❌ {notebook_config['notebook_name']} failed: {str(e)}", "❌")
            self.notebook_stats[notebook_config['notebook_name']]['failed'] += 1
            return False
    
    def cleanup(self):
        """Clean up temporary files"""
        try:
            if self.temp_dir.exists():
                shutil.rmtree(self.temp_dir)
                self.log("Temporary files cleaned up", "🧹")
        except Exception as e:
            self.log(f"Cleanup warning: {str(e)}", "⚠️")
    
    def execute_all_notebooks(self):
        """Execute all configured notebooks"""
        self.execution_count += 1
        self.last_execution_time = datetime.now(timezone.utc)
        
        print("\n")
        self.print_separator("═")
        print(f"{'CROSS-ACCOUNT KAGGLE EXECUTION':^70}")
        print(f"{'Execution Round #' + str(self.execution_count):^70}")
        print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC'):^70}")
        self.print_separator("═")
        print(f"  📊 Total Notebooks: {len(NOTEBOOKS)}")
        print(f"  🔀 Source: {SOURCE_ACCOUNT['username']}")
        print(f"  🎯 Destination: {DEST_ACCOUNT['username']}")
        self.print_separator("═")
        
        results = []
        
        try:
            for notebook_config in NOTEBOOKS:
                success = self.execute_notebook(notebook_config)
                results.append(success)
                print()
            
            successful = sum(results)
            failed = len(results) - successful
            
            self.print_separator("═")
            self.log(f"ROUND COMPLETED: {successful}/{len(NOTEBOOKS)} successful", "✅" if failed == 0 else "⚠️")
            self.print_separator("═")
            print()
            
            if failed == 0:
                self.success_count += 1
            else:
                self.failure_count += 1
            
        except KeyboardInterrupt:
            self.log("Execution interrupted", "⚠️")
            self.failure_count += 1
            raise
        except Exception as e:
            self.log(f"Execution failed: {str(e)}", "❌")
            self.failure_count += 1
        finally:
            self.cleanup()
    
    def get_runtime_stats(self):
        """Get runtime statistics"""
        uptime = datetime.now(timezone.utc) - self.start_time
        hours, remainder = divmod(uptime.total_seconds(), 3600)
        minutes, seconds = divmod(remainder, 60)
        
        success_rate = (self.success_count / self.execution_count * 100) if self.execution_count > 0 else 0
        
        if self.last_execution_time:
            next_exec = self.last_execution_time + timedelta(minutes=RUN_INTERVAL_MINUTES)
            time_until_next = next_exec - datetime.now(timezone.utc)
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
            'next': next_run_str,
            'notebook_stats': self.notebook_stats
        }

# ═══════════════════════════════════════════════════════════════════════════
# SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════

runner = CrossAccountKaggleRunner()

def scheduled_job():
    """Wrapper for scheduled execution"""
    try:
        runner.execute_all_notebooks()
        stats = runner.get_runtime_stats()
        
        print()
        print("═" * 70)
        print(f"{'📊 STATISTICS':^70}")
        print("═" * 70)
        print(f"  Execution Rounds: {stats['total']} (✅ {stats['success']} | ❌ {stats['failed']})")
        print(f"  Success Rate: {stats['rate']}")
        print(f"  Uptime: {stats['uptime']}")
        print(f"  Next Run: {stats['next']}")
        print()
        print("  Per-Notebook Stats:")
        for notebook_name, nb_stats in stats['notebook_stats'].items():
            print(f"    • {notebook_name}: ✅ {nb_stats['success']} | ❌ {nb_stats['failed']}")
        print("═" * 70)
        print()
    except Exception as e:
        print(f"\n❌ Scheduler error: {e}\n")
        sys.stdout.flush()

def main():
    """Main entry point"""
    try:
        print("\n🚀" * 35)
        print(f"{'CROSS-ACCOUNT KAGGLE NOTEBOOK RUNNER':^70}")
        print(f"{'GitHub Deployment Version':^70}")
        print("🚀" * 35)
        print(f"\n  ⏰ Schedule: Every {RUN_INTERVAL_MINUTES} minutes")
        print(f"  📚 Notebooks: {len(NOTEBOOKS)}")
        print(f"  🔀 Source Account: {SOURCE_ACCOUNT['username']}")
        print(f"  🎯 Destination Account: {DEST_ACCOUNT['username']}")
        print(f"  🕐 Started: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC")
        print()
        print("  Configured Notebooks:")
        for i, nb in enumerate(NOTEBOOKS, 1):
            print(f"    {i}. {nb['notebook_name']}")
            print(f"       └─ https://www.kaggle.com/code/{nb['dest_slug']}")
        print("\n" + "═" * 70 + "\n")
        
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
        print(f"  Total Rounds: {stats['total']} | Success: {stats['success']} | Failed: {stats['failed']}")
        print(f"  Uptime: {stats['uptime']}")
        print("═" * 70 + "\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ FATAL: {e}\n")
        sys.exit(1)

if __name__ == "__main__":
    main()
