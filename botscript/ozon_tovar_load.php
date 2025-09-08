<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];          // идентификатор клиента - строка в базе

$clientID = $data[clientID];        // Client ID Ozon
$apiKey = $data[apiKey];            // Api key Ozon


// echo $cabinet."----".$clientID."---".$apiKey."---------test";  // тестирование получения данных ключей


// Скрипт загрузки товаров



$limit = 1000;

// вытаскиваем poduct_id

$url = "https://api-seller.ozon.ru/v3/product/list";    //адрес запроса

$headers = [
        'Content-Type: application/json',
        'Client-Id:'.$clientID.'',
        'Api-Key:'.$apiKey.''
        ];                                              //headers параметры
        

$body = json_encode([
        
        'filter' => [
            
            'visibility' => 'ALL' 
            
            
            ],
    
        'limit' =>1000 
    
         ]);


                                                       
                                                        // curl запрос
    $ch = curl_init();   

    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    
    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE);   // получение статуса
    
    $product_list = [];                                 // массив с poduct_ID
    
    if ($status==200){                                  // тест статуса ответа API
        
         $array = json_decode($response, true);
         
         if (count($array)>0) {                         //если не пустой массив
         
               
                
         
         
                 foreach ($array['result']['items'] As $tovars) {          // перебор массива результата
                     
                      $product_id = $tovars['product_id'];
                      
                      
                      array_push($product_list, $product_id);             // записываем массив с product_id
                
                 }
                 
            
            echo count($product_list);
            
            
            
            
            
            // удаление из базы предыдущих записей по товару и кабинету
                
            $query = "DELETE from tovar  WHERE (cabinet='".$cabinet."')";
            $a = mysqli_query($link,$query);
         
            
            
            
            foreach ($product_list As $prod) {       
                
                $sku = $prod;
                
                $tovar = get_tov($sku);
                
          
                $tovar2 = get_tov2($sku);
                $prc = get_tov_price($sku);
               
               
               
                $id = $tovar['sku'];
                $title = $tovar['name'];
                
                $barcode = $tovar['barcode'];
                $img = $tovar['primary_image'];
              
                
                $old_price = str_replace('.', ',' , $prc['old_price']);
                $min_price = str_replace('.', ',' , $prc['min_price']);
                $marketing_price = str_replace('.', ',' , $prc['marketing_price']);
                $price = str_replace('.', ',' , $prc['price']);
                $marketing_seller_price = str_replace('.', ',' , $prc['marketing_seller_price']);
          
                $offer_id = $tovar['offer_id']; 
                $volume_weight = str_replace('.', ',' , $tovar['volume_weight']);
                
                $height = str_replace('.', ',' , $tovar2['height']);
                $depth = str_replace('.', ',' , $tovar2['depth']);
                $width = str_replace('.', ',' , $tovar2['width']);
                $weight = str_replace('.', ',' , $tovar2['weight']);
                
                $vol = ($height*$width*$depth)/1000000; 
                
                $vol = str_replace('.',',', $vol);
                
               
                //  запись в базу товаров 
                 
                $query = "INSERT INTO tovar (cabinet, sku, offer_id, title, barcode, old_price, min_price, marketing_price, price, marketing_seller_price, volume_weight, height, depth, width, weight, vol, img)  VALUES ('".$cabinet."', '".$id."', '".$offer_id."', '".$title."', '".$barcode."', '".$old_price."', '".$min_price."', '".$marketing_price."', '".$price."', '".$marketing_seller_price."', '".$volume_weight."', '".$height."', '".$depth."', '".$width."', '".$weight."', '".$vol."', '".$img."')"; 
                
                $a = mysqli_query($link,$query);
                
                echo "Загружено";
                
               
            
            }  
            
            
                
         }
         
    }
    
   

   
// вспомогательные функции 



    
function get_tov($idi) {                                                // функция вызова характеристик товара по prod_ID

global $clientID, $apiKey;

$url = "https://api-seller.ozon.ru/v3/product/info/list";    //адрес запроса

$headers = [
        'Content-Type: application/json',
        'Client-Id:'.$clientID.'',
        'Api-Key:'.$apiKey.''
        ];                                              //headers параметры

        
$body = json_encode([
        'product_id' => [$idi]
        ]);                                              //тело запроса
    


                                                       
                                                        // curl запрос
$ch = curl_init();   

    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    
    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE);
    
  
    
    if ($status==200){                                  // тест статуса ответа API
        
         $array = json_decode($response, true);
         
         //print_r ($array);
         
         
         if (count($array)>0) {                         //если не пустой массив
         
           
                
                 $sku =  $array['items'][0]['sources'][0]['sku'];        // парсим массив на значения
                 $name =  $array['items'][0]['name'];
                
                 $primary_image =   $array['items'][0]['primary_image'][0];
                 $marketing_price = str_replace('.',',', $array['items'][0]['marketing_price']);
                 $price = str_replace('.',',', $array['items'][0]['price']);
                 $minprice = str_replace('.',',', $array['items'][0]['min_price']);
                 $offer_id = $array['items'][0]['offer_id'];
                 $bc = $array['items'][0]['barcodes'];
                 $volume_weight = $array['items'][0]['volume_weight'];
                 
                 if (count($bc)>1) {                    // если баркод не один - разбираем массив и пишем в переменную через запятую
                     
                     $barcode = $bc[0].", ".$bc[1];
                     
                 } else {  $barcode = $bc[0]; }
                 
                 
        
         }
         
         
         
         $result = [
            
            'sku' =>  $sku, 
            'name' => $name, 
            'primary_image' => $primary_image,
            'marketing_price' => $marketing_price,
            'price' =>  $price, 
            'minprice' => $minprice, 
            'offer_id' => $offer_id,
            'barcode' => $barcode,
            'volume_weight' => $volume_weight
            
         ];   
         
       
         
         return $result;
         
         
    } 
  
}




function get_tov2($sku) {                                                // функция вызова дополнительных характеристик товара по prod_ID

global $clientID, $apiKey;

$url = "https://api-seller.ozon.ru/v4/product/info/attributes";    //адрес запроса

$headers = [
        'Content-Type: application/json',
        'Client-Id:'.$clientID.'',
        'Api-Key:'.$apiKey.''
        ];                                              //headers параметры
        
$body = '{

    "filter": {
        "product_id": ['.$sku.'],
        "visibility": "ALL"
    },
    "limit": 1000
    }';

                                                        // curl запрос
$ch = curl_init();   

    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    
    
    
    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE);
    

    if ($status==200){                                  // тест статуса ответа API
        
         $array = json_decode($response, true);
         
       
         
         if (count($array)>0) {                         //если не пустой массив
         
                
                 $height = str_replace('.',',', $array['result'][0]['height']);        // парсим массив на значения
                 $depth =  str_replace('.',',', $array['result'][0]['depth']);
                 $width =  str_replace('.',',', $array['result'][0]['width']);
                 $weight = str_replace('.',',', $array['result'][0]['weight']);
                 
     
         }
         
         $result = [
            
            'height' =>  $height, 
            'depth' => $depth, 
            'width' => $width,
            'weight' => $weight
            
            
         ];   
         
     
          
         return $result;
         
        
         
    } 
 
}




function get_tov_price($sku) {                                                // функция вызова дополнительных характеристик товара по prod_ID

global $clientID, $apiKey;

$url = "https://api-seller.ozon.ru/v5/product/info/prices";    //адрес запроса

$headers = [
        'Content-Type: application/json',
        'Client-Id:'.$clientID.'',
        'Api-Key:'.$apiKey.''
        ];                                              //headers параметры
        
$body = '{

    "filter": {
        "product_id": ['.$sku.'],
        "visibility": "ALL"
    },
    "limit": 1000
    }';

                                                        // curl запрос
$ch = curl_init();   

    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    
    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE);
    

    if ($status==200){                                  // тест статуса ответа API
        
         $array = json_decode($response, true);
         
         //print_r ($array);
         
         if (count($array)>0) {                         //если не пустой массив
         
                
                 $old_price =  str_replace('.',',', $array['items'][0]['price']['old_price']);        // парсим массив на значения
                 
                 $min_price =  str_replace('.',',',  $array['items'][0]['price']['min_price']); 
                 
                 $marketing_price =  str_replace('.',',',  $array['items'][0]['price']['marketing_price']); 
                 
                 $price =  str_replace('.',',',   $array['items'][0]['price']['price']); 
                 
                 $marketing_seller_price =  str_replace('.',',',  $array['items'][0]['price']['marketing_seller_price']); 
                 
               
     
         }
         
         $result = [
            
            'old_price' =>  $old_price,
            'min_price' =>  $min_price,
            'marketing_price' =>  $marketing_price,
            'price' =>  $price,
            'marketing_seller_price' =>  $marketing_seller_price
            
          
            
            
         ];   
         
      
         
         return $result;
         
    } 
 
}







    

    
   








?>