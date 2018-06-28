try:
    from .directorywatcher import DirectoryWatcher
except Exception as e:
    print("Warning: Not able to import directory watcher", e)

try:
    from .netwatcher import NetWatcher
except Exception as e:
    print("Warning: Not able to import net watcher", e)
