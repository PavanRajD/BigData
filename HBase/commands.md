$HBASE_HOME/bin/start-hbase.sh

hbase shell

describe "Movies"

scan "Movies", {'LIMIT' => 5}

alter 'Movies', {'NAME'=>'Product', 'VERSIONS'=>3}

put 'Movies', 'A1000YH725UROYB000RI4MA4', 'Product:text’, ‘sample 1.0’

put 'Movies', 'A1000YH725UROYB000RI4MA4', 'Product:text’, ‘sample 2.0'

get 'Movies', 'A1000YH725UROYB000RI4MA4', {'COLUMNS'=>'Product:text’, 'VERSIONS'=>3}

scan 'Movies', { 'COLUMNS' => 'Product:summary', 'FILTER' => "ValueFilter(=, 'substring:touching')"}
123 row(s) in 0.6810 seconds

scan 'Movies', { 'COLUMNS' => 'Product:text', 'FILTER' => "ValueFilter(=, 'regexstring:[^a-zA-Z\d\s:]')"}
97394 row(s) in 110.6050 seconds

scan 'Movies', { 'COLUMNS' => 'Product:score', 'FILTER' => "ValueFilter(=, 'substring:0.0')"}
0 row(s) in 0.4940 seconds

scan 'Movies', { 'COLUMNS' => 'Product:score', 'FILTER' => "ValueFilter(=, 'substring:3.0')"}
10002 row(s) in 10.8840 seconds

scan 'Movies', { 'COLUMNS' => 'Product:score', 'FILTER' => "ValueFilter(=, 'substring:5.0')"}
54277 row(s) in 21.1330 seconds
