<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data['cabinet'];          // идентификатор клиента - строка в базе

$apiKey = $data['apiKey'];        // Client ID Ozon


$query = "DELETE from tovar_wb  WHERE (cabinet='".$cabinet."')";
$a = mysqli_query($link,$query);


$limit = 100;

$product_list = [];   

$pars= true;

$updatedAt = "";

$nmId = "";

while ($pars==true){    



    $url = "https://content-api.wildberries.ru/content/v2/get/cards/list";
 
    $headers = [
        'Content-Type: application/json',
        'Authorization:'.$apiKey.''
      
        ]; 
        
    if ($updatedAt == ""){    
        
    $body = json_encode([
        
        'settings' => [
            
            'cursor' => ["limit" => $limit],
            'filter' => ["withPhoto" => -1]
            
            
            ]
    
         ]);
         
    } else { 
        
        $body = json_encode([
        
        'settings' => [
            
            'cursor' => ["limit" => $limit, "updatedAt" => $updatedAt, "nmId" =>$nmId ],
            'filter' => ["withPhoto" => -1]
            
            
            ]
    
         ]);
        

    }
    
    
    
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
    
   
    
     if ($status==200){  
        
        $array = json_decode($response, true);
        
        if (count($array)>0) {
                
                 foreach ($array['cards'] As $tovars) { 
                        
                        $artWB = $tovars['nmID'];
                        $vendorCode = $tovars['vendorCode'];
                        $subjectName= $tovars['subjectName'];
                        $brand = $tovars['brand'];
                        $title = $tovars['title'];
                        $video = $tovars['video'];
                        $description = $tovars['description'];
                        $photo = $tovars['photos'][0]['big'];
                        
                        $width = str_replace('.',',', $tovars['dimensions']['width']);
                        $height = str_replace('.',',', $tovars['dimensions']['height']);
                        $length = str_replace('.',',', $tovars['dimensions']['length']);
                        $weightBrutto = str_replace('.',',', $tovars['dimensions']['weightBrutto']);
                        
                      
                        $barcode = "";
                        $techSize = "";
                        $wbSize = "";
                        
                             foreach ($tovars['sizes'] As $sizes_array) { 
                                    
                                   $barcode = $barcode.$sizes_array['skus'][0].", ";
                                   $techSize = $techSize.$sizes_array['techSize'].", ";
                                   $wbSize = $wbSize.$sizes_array['wbSize'].", ";
                                  
                             }
                             
                        
                        $query = "INSERT INTO tovar_wb (cabinet, artWB, vendorCode, subjectName, brand, title, video, description, photo, width, height, length, weightBrutto, barcode, techSize, wbSize)  VALUES ('".$cabinet."', '".$artWB."', '".$vendorCode."', '".$subjectName."', '".$brand."', '".$title."', '".$video."', '".$description."', '".$photo."', '".$width."', '".$height."', '".$length."', '".$weightBrutto."', '".$barcode."', '".$techSize."', '".$wbSize."')"; 
                        
                        $a = mysqli_query($link,$query);
                        
        
        
        
                 }
                 
               
                 
                 $total = $array['cursor']['total'];
                 
                 if ($total < $limit) {
                        
                       
                        
                        $pars = false;
                        
                        
                        
                 } else {
                     
                  
                    
                    $updatedAt = $array['cursor']['updatedAt'];
                    $nmId = $array['cursor']['nmID'];
                     
                 }
                 
    
                
        }
     } else { return;}
     
     

}



?>