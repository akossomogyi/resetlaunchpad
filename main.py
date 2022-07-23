import copy
import os
import shutil
import sqlite3
import subprocess
import time
import pandas as pd


# order custom apps in alphabetic order:

def subproc(command: str, do_return: bool = False, sleep_after: float = 0.5):
    read_res = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True).stdout.read()
    if do_return:
        return read_res
    time.sleep(sleep_after)


def set_springboard_row(row_num: int, sleep_after: float = 0.5) -> None:
    subproc("defaults write com.apple.dock springboard-rows -int " + str(row_num), False, sleep_after)


def set_springboard_col(col_num: int, sleep_after: float = 0.5) -> None:
    subproc("defaults write com.apple.dock springboard-rows -int " + str(col_num), False, sleep_after)


def restart(sleep_after: float = 3.0) -> None:
    subproc("defaults write com.apple.dock ResetLaunchPad -bool true; killall Dock",False,)


def set_size(row_num: int = 5, col_num: int = 8, do_restart: bool = True) -> None:
    set_springboard_row(row_num)
    set_springboard_col(col_num)
    if do_restart:
        restart()


def get_launchpad_db():
    return subproc("echo $(getconf DARWIN_USER_DIR)/com.apple.dock.launchpad/db", True)


def set_size_odb(row_num=7, col_num=10):
    launchpad_db = get_launchpad_db()
    source_folder = str(launchpad_db.strip())[1:].replace("'", "")
    db = os.path.join(source_folder, "db")
    cp_db = os.path.join(os.getcwd(), "launchPad_db_cp")
    os.makedirs(cp_db, exist_ok=True)
    # make a copy of the db just in case:
    shutil.copy2(db, cp_db)
    set_size(row_num, col_num, False)
    shutil.copy2(cp_db, source_folder)
    subproc("killall Dock")


def reset_launchpad(init_reset=False, row_num=6, col_num=9):
    launchpad_db = get_launchpad_db()
    db = os.path.join(str(launchpad_db.strip())[1:].replace("'", ""), "db")
    shutil.copy2(db, os.getcwd())
    if init_reset:
        set_size(row_num, col_num)
    # get connection
    conn = sqlite3.connect(db)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM sqlite_master;")
    # get all objects
    objects = pd.DataFrame(cursor.fetchall())
    # human-readable columns
    objects.columns = [m[0] for m in cursor.description]
    tbl_data_list = []
    miss_data = []
    for tbl_name in objects[objects["type"] == "table"]["name"]:
        cursor.execute("SELECT * FROM " + tbl_name + ";")
        try:
            tbl_data = pd.DataFrame(cursor.fetchall())
            tbl_data.columns = [m[0] for m in cursor.description]
            tbl_data_list.append((tbl_name, tbl_data))
        except ValueError:
            miss_data.append(tbl_name)
    tbl_data_dict = dict(tbl_data_list)
    items = copy.deepcopy(tbl_data_dict["items"])
    # groups = copy.deepcopy(dict(tbl_data)["groups"])
    apps = copy.deepcopy(tbl_data_dict["apps"])
    items_mod = copy.deepcopy(items)
    items_mod["item_id"] = items_mod["rowid"]
    # 4 shows that it's an application, we're interested on the pages containing the apps:
    parents = sorted(list(items[items["type"] == 4]["parent_id"].drop_duplicates()))
    sort_by_parents = []
    for p in parents:
        under_page = items_mod[items_mod["parent_id"] == p]
        merged = pd.merge(under_page, apps)
        merged["Title"] = [f.upper() for f in merged["title"]]
        new_merged = merged.sort_values("Title").reset_index().reset_index()
        new_merged["ordering"] = [x for x in new_merged["level_0"]]
        sort_by_parents.append((p, new_merged[items.columns], new_merged))
    new_items = pd.concat([pd.concat([n for (m, n, o) in sort_by_parents]), items[items["type"] != 4]]).sort_values(
        "rowid")
    new_items.to_sql("items", conn, if_exists="replace")
    time.sleep(3.0)
    conn.close()
    subproc("killall Dock")


def main():
    reset_launchpad(True)


if __name__ == "__main__":
    main()
