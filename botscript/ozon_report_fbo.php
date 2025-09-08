<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];          // идентификатор клиента - строка в базе

$clientID = $data[clientID];        // Client ID Ozon
$apiKey = $data[apiKey];            // Api key Ozon

$df = $data[df];
$de = $data[de];

$mode =  $data[mode];

if ($mode ==1){
    
   
    
    echo "Load FBO from ".$df." to ". $de;
    
    report_fbo($df, $de);
    
}




function report_fbo($start_date_s,  $end_date_s){
    
     global $clientID, $apiKey, $cabinet, $link;
     
     //$end_date_s = date ("Y-m-d");

     //$start_date_s = date ("Y-m-d", strtotime($end_date_s. " - ".$dayload." day"));
     
     
     
     
     // удаление дат
    $query = "DELETE from report_fbo  WHERE (cabinet='".$cabinet."'  and created_at >='".$start_date_s."'  and created_at <='".$end_date_s."')";
    $a = mysqli_query($link,$query);
                    

     $offset=0; 
     $next_page = true;
     
     $result=[];
     
     $limit = 1000;
     
     $headers = [
        'Content-Type: application/json',
        'Client-Id:'.$clientID.'',
        'Api-Key:'.$apiKey.''
        ];  
    
     
     
     while ($next_page == true) {
         
        $payload = [
              'dir' => 'ASC',
              'filter' => [
                'since' => $start_date_s.'T00:00:00.000Z',
                'status' => '',
                'to' => $end_date_s.'T23:59:59.999Z'
               ],
               
              'limit' => $limit,
              'offset' => $offset,
              'translit' => true,
              'with' => [
                'analytics_data' => true,
                'financial_data' => true
              ]
            
    
    
            ];
         
        $body = json_encode($payload);
        
        $url = "https://api-seller.ozon.ru/v2/posting/fbo/list";    
    
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
            
            
            
            $data =  json_decode($response, true);
            
            if($data['result']) {
            
                $res_data = $data['result'];
                
               echo count($res_data);
                
                if(count($res_data) == $limit){         // если ответ сильно большой - запрос на следующую страницу
                    
                    $offset = $offset + $limit;
                    sleep(10);
                    
                } else {
                    
                    $next_page = false;
                    
                }
                
                    // парсинг ответа
                    
                    foreach ($res_data As $elem){
                        
                        $order_id = $elem['order_id'];
                        
                       
                        $order_number = $elem['order_number'];
                        $posting_number = $elem['posting_number'];
                        $status = $elem['status'];
                        $cancel_reason_id = $elem['cancel_reason_id'];
                        
                        $created_at = $elem['created_at'];
                        $created_at = date ("Y-m-d", strtotime($created_at));
                        
                        $in_process_at = $elem['in_process_at'];
                        
                        $sku = $elem['products'][0]['sku'];
                        $name = $elem['products'][0]['name'];
                        $quantity = $elem['products'][0]['quantity'];
                        
                        $price = str_replace('.', ',' , $elem['products'][0]['price']);
                        
                        
                        
                        $cluster_from = $elem['financial_data']['cluster_from'];
                        $cluster_to = $elem['financial_data']['cluster_to'];
                        
                        $delivery_type = $elem['analytics_data']['delivery_type'];
                        $is_premium = $elem['analytics_data']['is_premium'];
                        
                        $payment_type_group_name = $elem['analytics_data']['payment_type_group_name'];
                        $warehouse_id = $elem['analytics_data']['warehouse_id'];
                        
                        $warehouse_name = $elem['analytics_data']['warehouse_name'];
                        $is_legal = $elem['analytics_data']['is_legal'];
                        
                        
                        $query = "INSERT INTO report_fbo (cabinet, order_id, order_number, posting_number, status, cancel_reason_id, created_at, in_process_at, sku, name, quantity, price, cluster_from, cluster_to, delivery_type, is_premium, payment_type_group_name,  warehouse_id, warehouse_name, is_legal) VALUES ('".$cabinet."', '".$order_id."', '".$order_number."', '".$posting_number."', '".$status."', '".$cancel_reason_id."', '".$created_at."', '".$in_process_at."', '".$sku."', '".$name."', '".$quantity."', '".$price."', '".$cluster_from."', '".$cluster_to."', '".$delivery_type."', '".$is_premium."', '".$payment_type_group_name."', '".$warehouse_id."', '".$warehouse_name."', '".$is_legal."')";
                        $a = mysqli_query($link,$query);   
                        
                        
                        
                    }
                
            
            } else {
                
                $next_page = false;
                
            }
                
        }
         
        
     }
    
    
}





?>