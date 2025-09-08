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
    
    echo "Load FBS from ".$df." to ". $de;
    
    report_fbs($df, $de);
    
}




function report_fbs($start_date_s,  $end_date_s){
    
     global $clientID, $apiKey, $cabinet, $link;
     
     //$end_date_s = date ("Y-m-d");

     //$start_date_s = date ("Y-m-d", strtotime($end_date_s. " - ".$dayload." day"));
     
     // удаление дат
    $query = "DELETE from report_fbs  WHERE (cabinet='".$cabinet."'  and in_process_at >='".$start_date_s."'  and in_process_at <='".$end_date_s."')";
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
                'barcodes' => true,
                'financial_data' => true,
                'translit' => false
              ]
            
    
    
            ];
         
        $body = json_encode($payload);
        
        $url = "https://api-seller.ozon.ru/v3/posting/fbs/list";    
    
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
            
               $res_data = $data['result']['postings'];
               
               
                
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
                        $posting_number = $elem['posting_number'];
                        $order_number = $elem['order_number'];
                        
                        
                        $in_process_at = date ("Y-m-d", strtotime($elem['in_process_at']));
                        $shipment_date = $elem['shipment_date'];
                        $delivering_date = $elem['delivering_date'];
                        $delivery_date_begin = $elem['delivery_date_begin'];
                        $delivery_date_end = $elem['delivery_date_end'];
                        
                        $status = $elem['status'];                                  
                        $sku = $elem['products'][0]['sku'];
                        $offer_id = $elem['products'][0]['offer_id'];
                        $name = $elem['products'][0]['name'];
                        $quantity = $elem['products'][0]['quantity'];
                       
                        $old_price = str_replace('.', ',' , $elem['financial_data']['products'][0]['old_price']);       
                        $price = str_replace('.', ',' , $elem['financial_data']['products'][0]['price']);
                        
                        $total_discount_value = str_replace('.', ',' , $elem['financial_data']['products'][0]['total_discount_value']);             
                        
                        $total_discount_percent = str_replace('.', ',' , $elem['financial_data']['products'][0]['total_discount_percent']); 
                        
                        
                        
                        $delivery_method_name = $elem['delivery_method']['name'];
                        
                        $warehouse_name = $elem['delivery_method']['warehouse'];
                        
                        $tpl_provider = $elem['delivery_method']['tpl_provider'];
                        
                        $tracking_number = $elem['tracking_number'];
                        
                        $tpl_integration_type = $elem['tpl_integration_type'];
                        
                        $region_post_rfbs = $elem['analytics_data']['region'];         
                        
                        $city_post_rfbs = $elem['analytics_data']['city_post_rfbs'];
                        
                        $cluster_from = $elem['financial_data']['cluster_from'];
                        
                        $cluster_to = $elem['financial_data']['cluster_to'];
                       
                        $substatus = $elem['substatus'];
                        
                        
                        $query = "INSERT INTO report_fbs (cabinet, order_id, posting_number, order_number, in_process_at, shipment_date, delivering_date, delivery_date_begin, delivery_date_end, status, sku, offer_id, name, quantity, old_price, price, total_discount_value, total_discount_percent, delivery_method_name, warehouse_name, tpl_provider, tracking_number, tpl_integration_type,  region_post_rfbs, city_post_rfbs, cluster_from, cluster_to, substatus) VALUES ('".$cabinet."', '".$order_id."', '".$posting_number."', '".$order_number."', '".$in_process_at."', '".$shipment_date."', '".$delivering_date."', '".$delivery_date_begin."', '".$delivery_date_end."', '".$status."', '".$sku."', '".$offer_id."', '".$name."', '".$quantity."', '".$old_price."', '".$price."', '".$total_discount_value."', '".$total_discount_percent."', '".$delivery_method_name."', '".$warehouse_name."', '".$tpl_provider."', '".$tracking_number."', '".$tpl_integration_type."', '".$region_post_rfbs."', '".$city_post_rfbs."', '".$cluster_from."', '".$cluster_to."', '".$substatus."')";
                        
                     
                        $a = mysqli_query($link,$query);   
                        
                        
                        
                    }
                
            
            } else {
                
                $next_page = false;
                
            }
                
        }
         
        
     }
    
    
}





?>