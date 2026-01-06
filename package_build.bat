rmdir /s /q dist build src\loadex.egg-info 2>nul
pip install --upgrade build
python -m build
xcopy /s/y .\dist\*.whl d:\CAB\pythonpackages\