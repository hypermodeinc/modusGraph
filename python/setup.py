from setuptools import setup, Extension
import pybind11
import os
import subprocess
import shutil
import sys
from setuptools.command.build_ext import build_ext

# First, build the Go shared library
def build_go_lib():
    build_cmd = 'go build -buildmode=c-shared -o libmodusdb.dylib cgo_exports.go'
    result = subprocess.run(build_cmd.split(), capture_output=True, text=True)
    if result.returncode != 0:
        print("Go build failed:", result.stderr)
        raise RuntimeError('Failed to build Go shared library')
    return os.path.abspath('libmodusdb.dylib')

class CustomBuildExt(build_ext):
    def run(self):
        # Run the original build_ext command
        build_ext.run(self)
        
        # After building, copy the libraries to the package directory
        package_dir = 'modusdb'
        os.makedirs(package_dir, exist_ok=True)
        
        # Copy the Go shared library
        dylib_src = 'libmodusdb.dylib'
        if os.path.exists(dylib_src):
            shutil.copy2(dylib_src, os.path.join(package_dir, 'libmodusdb.dylib'))
        
        # Find and copy the built extension
        # Look in the specific lib directory where setuptools puts the built extension
        build_lib_dir = os.path.join('build', f'lib.{self.plat_name}-{".".join(map(str, sys.version_info[:2]))}')
        if os.path.exists(build_lib_dir):
            for file in os.listdir(build_lib_dir):
                if 'modusdb.cpython' in file and file.endswith('.so'):
                    extension_path = os.path.join(build_lib_dir, 'modusdb', file)
                    if os.path.exists(extension_path):
                        print(f"Copying extension from {extension_path} to {package_dir}")
                        shutil.copy2(extension_path, os.path.join(package_dir, file))
                        break
        
        # Create __init__.py if it doesn't exist
        init_path = os.path.join(package_dir, '__init__.py')
        if not os.path.exists(init_path):
            with open(init_path, 'w') as f:
                f.write('from .modusdb import Engine\n')

# Get the Go shared library path
go_lib = build_go_lib()

ext_modules = [
    Extension(
        "modusdb.modusdb",  # Changed to create module in single directory
        ["src/modusdb_bindings.cpp", "src/modusdb_wrapper.cpp"],
        include_dirs=[
            pybind11.get_include(),
            pybind11.get_include(user=True),
            "/opt/homebrew/opt/llvm/include/c++/v1",
            "/opt/homebrew/opt/nlohmann-json/include",
            "/opt/homebrew/Cellar/pybind11/2.13.6_1/include",
            "/opt/homebrew/opt/python@3.12/Frameworks/Python.framework/Versions/3.12/include/python3.12"
        ],
        library_dirs=[os.path.dirname(go_lib)],
        libraries=['modusdb'],
        extra_compile_args=[
            '-std=c++17',
            '-stdlib=libc++'
        ],
        extra_link_args=[
            '-stdlib=libc++',
            f'-Wl,-rpath,{os.path.abspath("modusdb")}'
        ],
        language='c++'
    )
]

setup(
    name="modusdb",
    ext_modules=ext_modules,
    install_requires=['pybind11>=2.6.0'],
    setup_requires=['pybind11>=2.6.0'],
    cmdclass={'build_ext': CustomBuildExt}
)