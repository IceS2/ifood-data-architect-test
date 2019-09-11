mkdir ./dist
cp ./src/main.py ./dist
cd ./src && zip -r ../dist/raw_layer.zip raw_layer && zip -r ../dist/datamarts.zip datamarts