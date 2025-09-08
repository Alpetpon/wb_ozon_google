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
    
    echo "Load FBO orders from ".$df." to ". $de."\n";
    
    $query = "DELETE from workdata WHERE (cabinet='".$cabinet."' and data_type='fbo_last_refresh')";
    $a = mysqli_query($link,$query);
    
    get_report($df, $de);
    
}


function create_report($start_date_s, $end_date_s){
    
    
    global $clientID, $apiKey, $cabinet, $link;


    //$end_date_s = date ("Y-m-d");

    //$start_date_s = date ("Y-m-d", strtotime($end_date_s. " - ".$colday." day"));

    $payload = [
    
        'filter' => [
            
                 'processed_at_from' => $start_date_s."T00:00:00.000Z",
                 'processed_at_to' => $end_date_s."T23:59:59.999Z",
                 'delivery_schema' => ['fbo'],
                 'sku' =>[],
                 'cancel_reason_id' =>[],
                 'offer_id' =>'',
                 'status_alias' =>[],
                 'statuses' =>[],
                 'title' =>''
            ],
        
        'language' => 'DEFAULT'
  
    ];

    $body = json_encode($payload);

    $headers = [
        'Content-Type: application/json',
        'Client-Id:'.$clientID.'',
        'Api-Key:'.$apiKey.''
        ];  
        
    $url = "https://api-seller.ozon.ru/v1/report/postings/create";    
    
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
         
         $report_id = $array['result']['code'];
         
         //echo $report_id;
         
         $query = "DELETE from workdata  WHERE (cabinet='".$cabinet."' and data_type='fbo_report')";
         $a = mysqli_query($link,$query);
        
         $query = "INSERT INTO  workdata (cabinet, data_type, data_values) VALUES ('".$cabinet."', 'fbo_report', '".$report_id."')";
         $a = mysqli_query($link,$query);
         
         
         // удаление данных их таблицы 
         
         return $report_id;
       
    }
    
    

}


function get_report($s_date, $e_date){
    
    global $cabinet, $link, $clientID, $apiKey;
    
        //$e_date = date ("Y-m-d");
        //$s_date = date ("Y-m-d", strtotime($e_date. " - ".$dayload." day"));
    
                                                                                                                            // проверка обновлено ли сегодня
        
        $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='fbo_last_refresh')"; 
        $request = mysqli_query($link,$query);
        $arr = mysqli_fetch_assoc($request);
     
        $date_refresh = $arr['data_values'];    
        
        $date_now = date ("Y-m-d");                                    
     
        if(!$date_refresh || $date_refresh!=$date_now ) {                                                                //если обновления не было
         
             $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='fbo_report')";                   // получаем ID отчета
             $request = mysqli_query($link,$query);
             $arr = mysqli_fetch_assoc($request);
             
             $report_id = $arr['data_values'];
                
                if (!$report_id) {                               // если ID Отчета не запрашивали - запросим
                    
                    
                     $report_id = create_report($s_date, $e_date);                                                                                             //вызываем создание отчета
                     sleep(10);
                    
                }
                
             //далее запрашиваем отчет
             
            $url = "https://api-seller.ozon.ru/v1/report/info";
            
            $payload = [
                    
                    'code' => $report_id
                 
                ];
                
            $headers = [
                
                        'Content-Type: application/json',
                        'Client-Id:'.$clientID.'',
                        'Api-Key:'.$apiKey.''
                        
                        ];  
            
            $body = json_encode($payload);
            
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
                
                $query = "DELETE from zakaz_fbo  WHERE (cabinet='".$cabinet."'  and c2 >='".$s_date."'  and c2 <='".$e_date."')";
                $a = mysqli_query($link,$query);
             
                $response = json_decode($response, true);
                
                $status = $response['result']['status'];
                $file = $response['result']['file'];
                
                //echo $file;
                
                if($status == "success" && $file){
                    
                    
                    
                  
                    
                    
                    
             
                    $data = file_get_contents($file);           //получаем csv по ссылке
                    
                    $rows = explode("\n",$data);                // переводим csv в массив
                    
                    //print_r ($rows);
                    
                    $csv_data =[];
                    
                    foreach($rows as $row) {
                        
                        $csv_data[] = str_getcsv($row, ";");
                        
                    }
                        
                        
                        
                        
                        
                       
                        
                        for($i=1; $i<count($csv_data); $i++){ 
                            
                            
                            $c0="";
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
                            $c16="";
                            $c17="";
                            $c18="";
                            $c19="";
                            $c20="";
                            $c21="";
                            $c22="";
                            $c23="";
                            $c24="";
                        
                            if($csv_data[$i][0]) {$c0 = $csv_data[$i][0];} else {$c0=0;}
                            
                            if($csv_data[$i][1]) {$c1 = $csv_data[$i][1];} else {$c1=0;}
                            
                            if($csv_data[$i][2]) {$c2 = $csv_data[$i][2];  $c2 = date ("Y-m-d", strtotime($c2)); } else {$c2=0;}
                            
                            if($csv_data[$i][3]) {$c3 = $csv_data[$i][3];} else {$c3=0;}
                            if($csv_data[$i][4]) {$c4 = $csv_data[$i][4];} else {$c4=0;}
                            if($csv_data[$i][5]) {$c5 = $csv_data[$i][5];} else {$c5=0;}
                            if($csv_data[$i][6]) {$c6 = $csv_data[$i][6];} else {$c6=0;}
                            if($csv_data[$i][7]) {$c7 = str_replace('.', ',' , $csv_data[$i][7]);} else {$c7=0;}
                            if($csv_data[$i][8]) {$c8 = $csv_data[$i][8];} else {$c8=0;}
                            if($csv_data[$i][9]) {$c9 = $csv_data[$i][9];} else {$c9=0;}
                            if($csv_data[$i][10]) {$c10 = $csv_data[$i][10];} else {$c10=0;}
                            if($csv_data[$i][11]) {$c11 = $csv_data[$i][11];} else {$c11=0;}
                            if($csv_data[$i][12]) {$c12 = str_replace('.', ',' , $csv_data[$i][12]);} else {$c12=0;}
                            if($csv_data[$i][13]) {$c13 = $csv_data[$i][13];} else {$c13=0;}
                            if($csv_data[$i][14]) {$c14 = str_replace('.', ',' , $csv_data[$i][14]);} else {$c14=0;}
                            if($csv_data[$i][15]) {$c15 = $csv_data[$i][15];} else {$c15=0;}
                            if($csv_data[$i][16]) {$c16 = $csv_data[$i][16];} else {$c16=0;}
                            if($csv_data[$i][17]) {$c17 = str_replace('.', ',' , $csv_data[$i][17]);} else {$c17=0;}
                            if($csv_data[$i][18]) {$c18 = str_replace('.', ',' , $csv_data[$i][18]);} else {$c18=0;}
                            if($csv_data[$i][19]) {$c19 = str_replace('.', ',' , $csv_data[$i][19]);} else {$c19=0;}
                            if($csv_data[$i][20]) {$c20 = str_replace('.', ',' , $csv_data[$i][20]);} else {$c20=0;}
                            if($csv_data[$i][21]) {$c21 = $csv_data[$i][21];} else {$c21=0;}
                            if($csv_data[$i][22]) {$c22 = str_replace('.', ',' , $csv_data[$i][22]);} else {$c22=0;}
                            if($csv_data[$i][23]) {$c23 = $csv_data[$i][23];} else {$c23=0;}
                            if($csv_data[$i][24]) {$c24 = str_replace('.', ',' , $csv_data[$i][24]);} else {$c24=0;}
                       
                            
                           
                            
                            if ($c0){
                            
                                  echo "\n".$c0;
                                   
                                   
                                 $query = "INSERT INTO zakaz_fbo (cabinet, c0, c1, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c19, c20, c21, c22, c23, c24)  VALUES ('".$cabinet."', '".$c0."', '".$c1."', '".$c2."', '".$c3."', '".$c4."', '".$c5."', '".$c6."', '".$c7."', '".$c8."', '".$c9."', '".$c10."', '".$c11."', '".$c12."', '".$c13."', '".$c14."', '".$c15."', '".$c16."', '".$c17."', '".$c18."', '".$c19."', '".$c20."', '".$c21."', '".$c22."', '".$c23."', '".$c24."')"; 
                                 
                                 $a = mysqli_query($link,$query);   
                                    
                            
                            }
                                                           
                        }
                        
                        
                        
                        
             
             
             //Ставим дату последнего обновления
             
             
             $query = "DELETE from workdata WHERE (cabinet='".$cabinet."' and data_type='fbo_last_refresh')";
             $a = mysqli_query($link,$query);
        
             $query = "INSERT INTO workdata (cabinet, data_type, data_values)  VALUES ('".$cabinet."','fbo_last_refresh','".$date_now."')";
             $a = mysqli_query($link,$query);  
             
             $query = "DELETE from workdata  WHERE (cabinet='".$cabinet."' and data_type='fbo_report')";
             $a = mysqli_query($link,$query);
             
             
              }
             
            }
             
        } 
        
        
                                                                                                                                // получаем данные по отчету
    
            
    
    
}



?>