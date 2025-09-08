<?

include 'sql.php'; //загрузка параметров MySQL


// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];          // идентификатор клиента - строка в базе

$apiKey = $data[apiKey];        // Client ID Ozon

$dF = $data[dF]; 


$query = "DELETE from rekl_wb  WHERE (cabinet='".$cabinet."' and dF = '".$dF."')";
$a = mysqli_query($link,$query);



$limit = 100;

$adverts_array = [];





$url="https://advert-api.wildberries.ru/adv/v1/promotion/count";

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
            
            
            
           
            
           
            foreach ($array['adverts'] as $adv){
                
                $status = $adv['status']; 
                
             
                 
                 
                if (($status==7) || ($status==9) || ($status==11)) {
                    
                    $type =  $adv['type'];
                    $advert_list =  $adv['advert_list'];
                    
        
                                
                             foreach ($advert_list as $advr){
                                 
                                $advertId = $advr['advertId'];
                                $date =  $advr['changeTime'];
                                $date = substr($date, 0, 10);
                               
                                $values = array($type, $status, $advertId, $date, 0);
                                
                                //echo $type."---".$status."---".$advertId."---".$date."<br>" ;
                                
                                array_push($adverts_array, $values);
                                    
                               
                                
                             }
                             
                            
                    }
                
              } 
           
            
            
          }
          
         

        }
        
    
      
        
        $add_data = [];
            
            if(count($adverts_array)>0){
                
                $idBlocks = [];
                $block = [];
                
                for($i=0; $i<count($adverts_array); $i++){
                    
                    array_push($block, $adverts_array[$i][2]);
                    
                    if((count($block)==50) or ($i == (count($adverts_array)-1))){
                         
                            
                            
                            array_push($idBlocks, $block);
                            
                            
                           
                            $block = [];
                        
                    }
                    
                }
                
                
            }
        
       
             
    
            
            $url = "https://advert-api.wildberries.ru/adv/v1/promotion/adverts";
            
            $headers = [
                'Content-Type: application/json',
                'Authorization:'.$apiKey.''
              
                ]; 
            
           
                
            for($i=0; $i<count($idBlocks); $i++){
            
                
                    sleep(1);
                    
                    $body = json_encode($idBlocks[$i]);
                    
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
                            
                        
                            
                           foreach ($array as $av){
                               
                               
                               
                               $id = $av['advertId'];
                               $name = $av['name'];
                               $startTime = substr($av['createTime'], 0, 10); 
                               $endTime = substr($av['endTime'], 0, 10); 
                               $budget = $av['dailyBudget'];
                               
                               array_push($add_data, array($id, $name, $startTime, $endTime, $budget));
                               
                               
                               
                            
                               
                           }
                          
                        }
                    }
                    
            }
            
    
    
    
    
    $full_adverts_array = [];
    
  
    
    for ($i=0; $i<count($adverts_array); $i++){
        
        $type_ = $adverts_array[$i][0];
        $status_ = $adverts_array[$i][1];
        $id_ = $adverts_array[$i][2];
        $change_ = $adverts_array[$i][3];
        $status_r = $adverts_array[$i][4];
        
        $name_ = customVLOOKUP($id_, $add_data, 1, "");
        $budget_ = customVLOOKUP($id_, $add_data, 4, "");
        
        $start_ = customVLOOKUP($id_, $add_data, 2, $dF);
        $end_ = customVLOOKUP($id_, $add_data, 3, $dF);
        
        //if($end_=="2100-01-01"){ $end_ = $dF;}
        
        array_push($full_adverts_array, array($type_, $status_, $id_, $change_, $status_r,  $name_,  $start_, $end_, $budget_));
        
       
        
        
    }

    $list = $full_adverts_array;
    
    $payload=[];
    $count = 1;
    

 
 
    $current_date = date("Y-m-d", strtotime($dF));
    
     
    $ln = count($list);
    
 

    for ($i=0; $i<$ln; $i++ ){
        
      
        if(($list[$i][4]==0) and ($count <= $ln)) {
            
            
            
            
            $id_current = $list[$i][2];
            
           
            $date_start = date("Y-m-d", strtotime($list[$i][6]));
            
            $date_end = date("Y-m-d", strtotime($list[$i][7]));
            
            
          
            
            
            if(($current_date >= $date_start) and ($current_date <= $date_end)) {
                
                
             
                
                $pl = ["id" => $id_current, "dates" => [$dF, $dF] ];
                
              
                
                array_push($payload, $pl);
                
                $count++;
                
            }
            
            
            
           
            
            
        }
        
        
        
    }
    
    
   
            
    if(count($payload)>0){
                
                $payloadBlocks = [];
                $block = [];
                
                for($i=0; $i<count($payload); $i++){
                    
                    array_push($block, $payload[$i]);
                    
                    if((count($block)==100) or ($i == (count($payload)-1))){
                         
                            
                            
                            array_push($payloadBlocks, $block);
                            
                            
                           
                            $block = [];
                        
                    }
                    
                }
                
                
    }
    
    
   
    
    
    
    
    
    
    
    
    
    
    
    
    
 
    if(count($payloadBlocks) == 0) {
        
        exit("Нет РК для анализа");
        
    }
    
    
    
    
     for($i=0; $i<count($payloadBlocks); $i++){
            
                
            
    
    
            $body = json_encode($payloadBlocks[$i]);
            
            echo "Запрос блок ".$i."<br><br>";
            
            
            if(count($payload)>0){
                
            $headers = [
                        'Content-Type: application/json',
                        'Authorization:'.$apiKey.''
                      
                        ]; 
            
            
            $url = "https://advert-api.wildberries.ru/adv/v2/fullstats";   
            
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
                               
                                echo $status."<br>";
                
                                $array = json_decode($response, true);
                                
                                if (count($array)>0) {
                                
                                    foreach ($array as $fullstats){
                                        
                                        $idr = $fullstats['advertId'];
                                        
                                        $typid = 0;
                                        $adname = "";
                                        
                                        for ($tt=0; $tt < $ln; $tt++ ){
                                            
                                            if($list[$tt][2] == $idr){
                                            $typid = $list[$tt][0];
                                            $adname = $list[$tt][5];
                                            break;
                                            }
                                            
                                        }
                                        
                                        
                                        
                                      
                                        
                                        
                                        $days_list = $fullstats['days'];
                                        
                                            foreach ($days_list as $dys) {
                                                
                                                $apps = $dys['apps'];
                                                    
                                                    foreach($apps as $apps2) {
                                                        
                                                        $nm = $apps2['nm'];
                                                        
                                                             foreach($nm as $nms){
                                                                 
                                                                 
                                                                 $advertId = $idr;
                                                                 $dFs =  date("Y-m-d", strtotime($dys['date']));
                                                                 $appType = $nm = $apps2['appType'];
                                                                 $nmId = $nms['nmId'];
                                                                 $name = $nms['name'];
                                                                 $views = $nms['views'];
                                                                 $clicks = $nms['clicks'];
                                                                 $ctr = str_replace('.',',', $nms['ctr']);
                                                                 $cpc = str_replace('.',',', $nms['cpc']);
                                                                 $sum = str_replace('.',',', $nms['sum']);
                                                                 $atbs = str_replace('.',',', $nms['atbs']);
                                                                 $orders = str_replace('.',',', $nms['orders']);
                                                                 $cr  = str_replace('.',',', $nms['cr']);
                                                                 $shks = str_replace('.',',', $nms['shks']);
                                                                 $sum_price = str_replace('.',',', $nms['sum_price']);
                                                                 
                                                                 
                                                                $query = "INSERT INTO rekl_wb (cabinet, dF, advertId ,adname, typid, appType, nmId, name, views, clicks, ctr, cpc, sum, atbs, orders, cr, shks, sum_price)  VALUES ('".$cabinet."', '".$dFs."', '".$advertId."', '".$adname."', '".$typid."', '".$appType."', '".$nmId."', '".$name."', '".$views."', '".$clicks."', '".$ctr."', '".$cpc."', '".$sum."', '".$atbs."', '".$orders."', '".$cr."', '".$shks."', '".$sum_price."')"; 
                                
                                                                 
                                                                
                                
                                                                 $a = mysqli_query($link,$query);
                
                
                                                                 
                                                                 
                                                                 
                                                                 
             
             
             
                                                                 
                                                             } 
                                                        
                                                        
                                                    }
                                                
                                                
                                                
                                            }
                                            
                                            
                                        
                                        
                                        
                                    }
                                        
                
                            
                
                                }
                        
                           }
                
                
                
            }
            
            
        sleep(20);
    
    
     }
    
    
    function customVLOOKUP($searchValue, $dataArray, $columnToReturn, $defaultValue) {
    
    for($i=0; $i<count($dataArray); $i++){
        
        $row = $dataArray[$i];
            
            if($row[0]==$searchValue) {
                return $row[$columnToReturn];
            }
    }
    
    return $defaultValue;
    
    
    
}    
      
?>
