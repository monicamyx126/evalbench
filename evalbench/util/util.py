import os
import shutil
import logging
from git import Repo


def clone(repo_dir, repo_url):
    if os.path.exists(repo_dir):
        logging.info(f"Repository directory '{repo_dir}' exists. Deleting it...")
        shutil.rmtree(repo_dir)
    logging.info(f"Cloning '{repo_url}' to '{repo_dir}'...")
    Repo.clone_from(repo_url, repo_dir)
