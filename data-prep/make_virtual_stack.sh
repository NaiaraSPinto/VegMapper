aws s3 ls servir-stacks/para/2020/all-bands/ | awk '{print $4}'> tiles_list.txt

prefix=/vsis3/servir-stacks/para/2020/all-bands/

awk -v prefix="$prefix" '{print prefix $0}'  tile_list.txt > para_tiles.txt

gdalbuildvrt -input_file_list para_tiles.txt /vsis3/servir-stacks/para/2020/all-bands/para_virtual_stack_2020.vrt