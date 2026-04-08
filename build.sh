# Clean out old cached configuration and compiled artifacts 
# Navigate to the build directory
rm -rf build && mkdir -p build

cd build

# Generate configuration 
cmake ..

# Compile using all available CPU threads
make -j$(nproc)