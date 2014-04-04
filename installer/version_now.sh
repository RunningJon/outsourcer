set -e
source version.sh

echo versioning $os_version

cd ..
echo removing old version
rm -rf history/$os_version/

echo create new version folder
mkdir history/$os_version

echo create new bin folder
mkdir history/$os_version/bin

echo copy bin folder
cp bin/* history/$os_version/bin/

echo create new jar folder
mkdir history/$os_version/jar

echo copy jar folder
cp jar/* history/$os_version/jar/

echo create new sql folder
mkdir history/$os_version/sql

echo copy sql folder
cp -R sql/* history/$os_version/sql/

echo create java folder
mkdir history/$os_version/java

echo copy java folder
cp *.java history/$os_version/java/

echo copy README.txt
cp README.txt history/$os_version/

echo copy license files
cp LICENSE.txt history/$os_version/

echo copy os_path
cp os_path.sh history/$os_version/

echo copy os_install
cp os_install.sh history/$os_version/

echo version $os_version created!

echo create new archive
zip -r os.zip bin/* jar/*.jar log sql/* README.txt LICENSE.txt os_install.sh os_path.sh

echo move zip to installer directory
mv os.zip installer/
cd installer

echo create archive for distribution
zip $os_version.zip os.zip version.sh install.sh 

echo remove temporary archive
rm os.zip

echo move new zip to history
mv $os_version.zip ../history/
echo Complete!!
