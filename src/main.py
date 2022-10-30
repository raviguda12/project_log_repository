import os
import sys
from pathlib import Path
sys.path.append(str(Path.cwd().parent))
import env

if __name__ == "__main__":
	exec(open(r"{}/{}".format(os.getcwd(), env.isi_py_file)).read())
	# exec(open(r"{}/{}".format(os.getcwd(), env.kss_py_file)).read())