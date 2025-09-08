<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data['cabinet'];          
$apiKey = $data['apiKey'];        
$dF = $data['dF']; 
$dE = $data['dT']; 







$query = "DELETE from voronka_wb  WHERE (cabinet='".$cabinet."' and dF >= '".$dF."' and dF <= '".$dE."')";
$a = mysqli_query($link,$query);







$url = "https://seller-analytics-api.wildberries.ru/api/v2/nm-report/detail";
 
$headers = [
        'Content-Type: application/json',
        'Authorization:'.$apiKey.''
      
        ]; 

$page = 1;
$next_page = true;

while ($next_page==true){
    
 
     
     $body = json_encode([
        
        'period' => [
            
           'begin' => $dF." 00:00:00",
           'end' => $dE." 23:59:59"
            
            
            ],
        'page' => $page
     ]);
      
    
          
            
	
    
    
   
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
    
    
	echo $response;
	
    
     if ($status==200){  
        
        $array = json_decode($response, true);
        
      
        if (count($array)>0) {
    
            foreach($array['data']['cards'] as $data)   {
                
                $nmID = $data['nmID'];
                
                $vendorCode = $data['vendorCode'];
                
                $brandName = $data['brandName'];
                
                $object_id = $data['object']['id'];
                
                $object_name = $data['object']['name'];
                
                $openCardCount = $data['statistics']['selectedPeriod']['openCardCount'];
                
                $addToCartCount = $data['statistics']['selectedPeriod']['addToCartCount'];
                
                $ordersCount = $data['statistics']['selectedPeriod']['ordersCount'];
                
                $buyoutsCount= $data['statistics']['selectedPeriod']['buyoutsCount'];
                
                $cancelCount = $data['statistics']['selectedPeriod']['cancelCount'];
                
                $ordersSumRub = $data['statistics']['selectedPeriod']['ordersSumRub'];
                
                $buyoutsSumRub = $data['statistics']['selectedPeriod']['buyoutsSumRub'];
                
                $cancelSumRub = $data['statistics']['selectedPeriod']['cancelSumRub'];
                
                $stocksWb = $data['stocks']['stocksWb'];
                
                $stocksMp = $data['stocks']['stocksMp'];
                
                
                $ordersSumRub = str_replace('.',',', $ordersSumRub);
                
                $cancelSumRub = str_replace('.',',', $cancelSumRub);
                
               
                $query2 = "INSERT INTO voronka_wb (cabinet, dF, nmID, vendorCode, brandName, object_id, object_name, openCardCount, addToCartCount, ordersCount, buyoutsCount, cancelCount, ordersSumRub, buyoutsSumRub, cancelSumRub, stocksWb, stocksMp)  VALUES ('".$cabinet."', '".$dF."', '".$nmID."','".$vendorCode."', '".$brandName."', '".$object_id."', '".$object_name."', '".$openCardCount."', '".$addToCartCount."', '".$ordersCount."', '".$buyoutsCount."', '".$cancelCount."', '".$ordersSumRub."', '".$buyoutsSumRub."', '".$cancelSumRub."','".$stocksWb."','".$stocksMp."')";
				
				
				
                $a = mysqli_query($link, $query2);
                
              
            }
           
        
           if($array['data']['isNextPage']==false)
                
                { $next_page=false; 
                    
            } else {
                
                 $page++;
                
            }
           
            
    
        }
        
     }
     
}



?>