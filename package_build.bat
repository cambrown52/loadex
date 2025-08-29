rmdir /s/q dist
rmdir /s/q build 
pip install --upgrade build
python -m build
xcopy /s/y .\dist\*.whl d:\CAB\pythonpackages\