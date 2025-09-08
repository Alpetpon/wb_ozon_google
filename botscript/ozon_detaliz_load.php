<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];          // идентификатор клиента - строка в базе

$clientID = $data[clientID];        // Client ID Ozon
$apiKey = $data[apiKey];            // Api key Ozon

$df =  $data[df]; 
$de =  $data[de]; 


//echo $cabinet."----".$clientID."---".$apiKey."---------test";  // тестирование получения данных ключей


// Скрипт загрузки детализации


//удаление дат по кабинету, для избежания задвоения

//$df = "2025-01-01";
//$de = "2025-01-04";







get_detaliz_load($df, $de);





function get_detaliz_load($startday, $endday) {
    
global $clientID, $apiKey, $cabinet, $link, $de, $df;



$query = "DELETE from detaliz  WHERE (cabinet='".$cabinet."' and c1 >='".$df."'  and c1 <='".$de."')";
$a = mysqli_query($link,$query); 

echo $query;

$start_date_s = $startday;
$end_date_s  = $endday;

$resp = get_d($start_date_s, $end_date_s, 1);

$data = json_decode($resp, true);

$count = $data['result']['page_count'];         //запрашивем сколько страниц в массиве


    for ($i=1;  $i<=$count; $i++){
        
        $resp = get_d($start_date_s, $end_date_s, $i);
        
        
        
        $data = json_decode($resp, true);
        $result_sk =  $data['result']['operations'];
        
        //print_r($result_sk);
        
       
        
        foreach($result_sk As $elem){
            
            
          
            $c1="";
            $c2="";
            $c3="";
            $c4="";
            $c5="";
            $c6="";
            $c7="";
            $c8="";
            $c9="";
            $c10="";
            $c11="";
            $c12="";
            $c13="";
            $c14="";
            $c15="";
            $c16=0;
            $c17="";
            $c18="";
            $c19="";
            $c20="";
            $c21="";
            $c22="";
            $c23="";
            $c24="";
            
            
            
            
             
             
            
            
            //дата начисления
            
            $operation_date = $elem['operation_date'];  
            $operation_date = date("Y-m-d", strtotime($operation_date));
            $c1 =  $operation_date;
           
            //тип начисления 
            $operation_type_name = $elem['operation_type_name'];
            $c2=$operation_type_name;
            
             // Номер отправления или идентификатор услуги
            $posting_number = $elem['posting']['posting_number'];
            $c3 =   $posting_number;
            
             // Дата оказания услуги
            $order_date = $elem['posting']['order_date']; 
            if ($order_date!=""){
                
                $order_date = date("Y-m-d", strtotime($order_date));
                $c4 =  $order_date;
            }
            
             // Склад отгрузки
            $delivery_schema = $elem['posting']['delivery_schema'];
            $c5 = $delivery_schema;
            
             // sku & name
            $item = $elem['items'];
                
                foreach( $item As $el){
                    
                   $name =  $el['name'];     //наименование         
                   $sku = $el['sku'];        //sku 
                   
                   $c8 = $name;
                   $c6 = $sku; 
                
             
                }
            
            
           //количество
                $cols = count($item);
                $c9 = $cols;
            
            
            //запродажувозврат до вычета услуг
            
            $accruals_for_sale = $elem['accruals_for_sale']; 
            $c10 = str_replace('.', ',' , $accruals_for_sale);  
           
             //комиссия
            $sale_commission = $elem['sale_commission'];     //Комиссия за продажу или возврат комиссии за продажу.
            $c12 = str_replace('.', ',' , $sale_commission);
            
           
            
            $serv = $elem['services'];
			
				
            
            
                foreach($serv As $ele){
                    
                  $name = $ele['name'];
                  $pric = $ele['price'];
                    
                    if ($name=="MarketplaceServiceItemFulfillment") {$c13 = str_replace('.', ',' , $pric);} //— сборка заказа.
                    
                    
                    if ($name=="MarketplaceServiceItemDropoffFF") {$c14 = str_replace('.', ',' , $pric);}       //— услуга drop-off в пункте приёма заказов.
                    if ($name=="MarketplaceServiceItemDropoffPVZ") {$c14 = str_replace('.', ',' , $pric);} 
                    if ($name=="MarketplaceServiceItemDropoffSC") {$c14 = str_replace('.', ',' , $pric);} 
                    
                    
                    
                    if ($name=="MarketplaceServiceItemDirectFlowTrans") {$c15 = str_replace('.', ',' , $pric);} //— магистраль.
                    
                    
					
					
                    if ($name=="MarketplaceServiceItemDelivToCustomer") {$c16 = str_replace('.', ',' , $pric);} //— последняя миля.
					
					
					
					
					
					if ($name=="MarketplaceServiceItemRedistributionLastMileCourier") {$c16 = $c16+(float)$pric;} ;
						
					if ($name=="MarketplaceServiceItemRedistributionLastMilePVZ") {$c16 = $c16+(float)$pric;} ;
                    
					
					
					
                    if ($name=="MarketplaceServiceItemReturnFlowTrans") {$c17 = str_replace('.', ',' , $pric);} //— обратная магистраль.
                    
                    
                    
                    if ($name=="MarketplaceServiceItemReturnAfterDelivToCustomer") {$c18 = str_replace('.', ',' , $pric);} //— обработка возврата.
                    if ($name=="MarketplaceServiceItemRedistributionReturnsPVZ") {$c18 = str_replace('.', ',' , $pric);} //— обработка возврата.
                    
                    
                    
                    
                    if ($name=="MarketplaceServiceItemReturnNotDelivToCustomer") {$c19 = str_replace('.', ',' , $pric);}  //— обработка отмен.
                    
                    if ($name=="MarketplaceServiceItemReturnPartGoodsCustomer") {$c20 = str_replace('.', ',' , $pric);}  //— обработка невыкупа. 
                    
                    if ($name=="MarketplaceServiceItemDirectFlowLogistic") {$c21 = str_replace('.', ',' , $pric);} //— логистика.
                    
                    if ($name=="MarketplaceServiceItemReturnFlowLogistic") {$c23 = str_replace('.', ',' , $pric);}  //— обратная логистика.
                    
                    
                    
                }
                
                  
                
                
            
                
                
               $amount = $elem['amount'];
               
               $c24 = str_replace('.', ',' , $amount);
			   
			   $c16 = strval($c16);
               
               $c16 = str_replace('.', ',' , $c16);
               
              
               $query = "INSERT INTO detaliz (cabinet, c1, c2, c3, c4, c5, c6,  c8, c9, c10, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21,  c23, c24)  VALUES ('".$cabinet."', '".$c1."', '".$c2."', '".$c3."', '".$c4."', '".$c5."', '".$c6."',  '".$c8."', '".$c9."', '".$c10."', '".$c12."', '".$c13."', '".$c14."', '".$c15."', '".$c16."', '".$c17."', '".$c18."', '".$c19."', '".$c20."', '".$c21."',  '".$c23."', '".$c24."')"; 
               $a = mysqli_query($link,$query);
                
             
               
            
        }
       
       
        
        
    }
    
    
    
   
       
        
}








// запрос сырых данных по детализации

function get_d($s_date, $nd_date, $pag) {
 
global $clientID, $apiKey;   
    
$url = "https://api-seller.ozon.ru/v3/finance/transaction/list";    //адрес запроса

$headers = [
        'Content-Type: application/json',
        'Client-Id:'.$clientID.'',
        'Api-Key:'.$apiKey.''
        ];                                              //headers параметры
        
$body = '{
    "filter": {

        "date": {

            "from":"'.$s_date.'T00:00:00.000Z",
            "to":"'.$nd_date.'T23:59:59.999Z"
            
       },
       
       "operation_type": [ ],
       "posting_number": "",
       "transaction_type": "all"
  
    },
    
  "page":'.$pag.',
  "page_size": 1000
  
    }';
    
  
    
    
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
    
    echo $status;

    if ($status==200){     
        
           
            return $response;
    
        
    }

}



?>
