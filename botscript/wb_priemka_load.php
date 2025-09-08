<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];          // идентификатор клиента - строка в базе

$apiKey = $data[apiKey];       

$dF = $data[dF]; 

echo "Платная приемка \n";

$query = "DELETE from stock_wb  WHERE (cabinet='".$cabinet."' and dF = '".$dF."')";

$a = mysqli_query($link,$query);




$url = "https://seller-analytics-api.wildberries.ru/api/v1/analytics/acceptance-report?dateFrom=".$dF."&dateTo=".$dF."";

echo $url;
 
 
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
        
        if (count($array['report'])>0) {
          
            foreach ($array['report'] as $priem){
                
                $count = $priem['count'];
                $giCreateDate = $priem['giCreateDate'];
                $incomeId = $priem['incomeId'];
                $nmID = $priem['nmID'];
                $shkСreateDate = $priem['shkСreateDate'];
                $subjectName = $priem['subjectName'];
                $total = $priem['total'];
                
                 $query = "INSERT INTO priem_wb (cabinet, dF, count, giCreateDate, incomeId,  nmID, shkСreateDate, subjectName, total)  VALUES ('".$cabinet."', '".$dF."', '".$count."', '".$giCreateDate."', '".$incomeId."', '".$nmID."', '".$shkСreateDate."', '".$subjectName."', '".$total."')"; 
                        
                $a = mysqli_query($link,$query); 
                     
                
            }
          
          
          
          
        }
        
        
    }




?>