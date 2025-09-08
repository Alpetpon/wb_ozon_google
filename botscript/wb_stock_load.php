<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];          // идентификатор клиента - строка в базе

$apiKey = $data[apiKey];        // Client ID Ozon

$dF = date ("Y-m-d");

$query = "DELETE from stock_wb  WHERE (cabinet='".$cabinet."' and dF = '".$dF."')";

$a = mysqli_query($link,$query);


$url = "https://statistics-api.wildberries.ru/api/v1/supplier/stocks?dateFrom=2024-01-01";
 
 
$headers = [
        'Content-Type: application/json',
        'Authorization:'.$apiKey.''
      
        ]; 


    $ch = curl_init();   
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    $response = curl_exec($ch);
    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE);
    
 
    
    if ($status==200){  
        
        $array = json_decode($response, true);
        
        if (count($array)>0) {
          
         
          
            foreach ($array as $stock) {
                
                
                    $nmId = $stock['nmId'];
                    $barcode = $stock['barcode'];
                    $techSize = $stock['techSize'];
                    $warehouseName = $stock['warehouseName'];
                    $quantity = $stock['quantity'];
                    $inWayToClient = $stock['inWayToClient'];
                    $inWayFromClient = $stock['inWayFromClient'];
                    $quantityFull = $stock['quantityFull'];
                
                
                         
                     
                    $query = "INSERT INTO stock_wb (cabinet, dF, nmId, barcode, techSize, warehouseName, quantity, inWayToClient, inWayFromClient, quantityFull)  VALUES ('".$cabinet."', '".$dF."', '".$nmId."', '".$barcode."', '".$techSize."', '".$warehouseName."', '".$quantity."', '".$inWayToClient."', '".$inWayFromClient."', '".$quantityFull."')"; 
                        
                    $a = mysqli_query($link,$query); 
                     
                
            }  
       
          
        
       




        }
    }









?>