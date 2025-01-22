import os
import sqlite3
import sys

def get_user_version(conn):
    """Retrieve the current user_version from the SQLite database."""
    cur = conn.cursor()
    cur.execute("PRAGMA user_version;")
    return cur.fetchone()[0]

def set_user_version(conn, version):
    """Set the user_version in the SQLite database."""
    cur = conn.cursor()
    cur.execute(f"PRAGMA user_version = {version};")
    conn.commit()

def run_migrations(database_path, migrations_dir):
    """
    Run migrations from the specified directory based on the current user_version.

    Args:
        database_path (str): Path to the SQLite database file.
        migrations_dir (str): Path to the directory containing migration SQL files.
    """
    conn = sqlite3.connect(database_path)
    try:
        current_version = get_user_version(conn)

        migration_files = sorted(
            [f for f in os.listdir(migrations_dir) if f.startswith("m") and f.endswith(".sql")]
        )

        for file in migration_files:
            migration_version = int(file[1:-4])
            if migration_version > current_version:
                print(f"Running migration: {file}")
                try:
                    with open(os.path.join(migrations_dir, file), "r") as sql_file:
                        sql_script = sql_file.read()
                        conn.executescript(sql_script)

                    set_user_version(conn, migration_version)
                    print(f"Updated user_version to {migration_version}")
                except sqlite3.Error as e:
                    print(f"Error occurred while running migration {file}: {e}")
                    print("Aborting migrations.")
                    break

    finally:
        conn.close()

def rollback_migrations(database_path, migrations_dir, target_version):
    """
    Roll back migrations from the current user_version to the target version.

    Args:
        database_path (str): Path to the SQLite database file.
        migrations_dir (str): Path to the directory containing rollback SQL files.
        target_version (int): The user_version to roll back to.
    """
    conn = sqlite3.connect(database_path)
    try:
        current_version = get_user_version(conn)
        print(f"Current user_version: {current_version}")
        print(f"Target rollback version: {target_version}")

        if target_version >= current_version:
            print("Target version must be less than the current user_version.")
            return

        rollback_files = sorted(
            [f for f in os.listdir(migrations_dir) if f.startswith("r") and f.endswith(".sql")],
            reverse=True
        )

        for file in rollback_files:
            rollback_version = int(file[1:-4])
            if current_version > rollback_version >= target_version:
                print(f"Running rollback: {file}")
                try:
                    with open(os.path.join(migrations_dir, file), "r") as sql_file:
                        sql_script = sql_file.read()
                        conn.executescript(sql_script)

                    set_user_version(conn, rollback_version)
                    print(f"Rolled back to version {rollback_version}")
                except sqlite3.Error as e:
                    print(f"Error occurred while running rollback {file}: {e}")
                    print("Aborting rollbacks.")
                    break

        print("Rollback process complete.")
    finally:
        conn.close()

def main():
    if len(sys.argv) < 3:
        print("Usage:")
        print("  To migrate: python3 bc.py m [database file] [migration dir]")
        print("  To rollback: python3 bc.py r [target version] [database file] [migration dir]")
        sys.exit(1)

    action = sys.argv[1].strip().lower()
    if action not in ("m", "r"):
        print("Invalid action. Use 'm' for migrate or 'r' for rollback.")
        sys.exit(1)

    database_path = sys.argv[2]
    migrations_dir = sys.argv[3] if len(sys.argv) > 3 else "migrations"

    if action == "m":
        print("Starting migrations...")
        run_migrations(database_path, migrations_dir)
    elif action == "r":
        if len(sys.argv) < 4:
            print("For rollback, you must specify a target version.")
            sys.exit(1)
        target_version = int(sys.argv[2])
        database_path = sys.argv[3]
        migrations_dir = sys.argv[4] if len(sys.argv) > 4 else "migrations"
        print(f"Rolling back to version {target_version}...")
        rollback_migrations(database_path, migrations_dir, target_version)

if __name__ == "__main__":
    main()
