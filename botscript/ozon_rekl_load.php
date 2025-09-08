<?

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$cabinet = $data[cabinet];          // идентификатор клиента - строка в базе

$clientID = $data[clientID];        // Client ID Ozon
$apiKey = $data[apiKey];            // Api key Ozon

$advert_client_id = $data[advert_client_id];
$client_secret = $data[client_secret];

$df = $data[df];

$de = $data[de];

$dn = date ("Y-m-d");  





$mode =  $data[mode];

 


if ($mode == 1){                 /// вызывается при первичной загрузке данных

    
   echo "первичная загрузка данных РК с ".$df." по ".$de." Обнуляем все данные и грузим заново \n";
   
  
   $query = "DELETE from workdata WHERE cabinet='".$cabinet."' and data_type='date_rk_from'";
   $a = mysqli_query($link,$query);
   
   $query = "DELETE from workdata WHERE cabinet='".$cabinet."' and data_type='date_rk_to'";
   $a = mysqli_query($link,$query);
   
   $query = "DELETE from workdata WHERE cabinet='".$cabinet."' and data_type='all_rk_load'";
   $a = mysqli_query($link,$query);
   
   $query = "DELETE from workdata WHERE cabinet='".$cabinet."' and data_type='rekl_id_report'";
   $a = mysqli_query($link,$query); 
   
   $query = "DELETE from workdata WHERE (cabinet='".$cabinet."' and data_type='rekl_data_refresh')";
   $a = mysqli_query($link,$query);  
   
   
   // удаяем лист со всеми РК по этому кабинету
   
    $query = "DELETE from list_rk  WHERE (cabinet='".$cabinet."')";
    $a = mysqli_query($link, $query);
   
   
   // устанавливаем дату, с которой пошла загрузка
   
   $query = "INSERT INTO workdata (cabinet, data_type, data_values)  VALUES ('".$cabinet."','date_rk_from','".$df."')";
   $a = mysqli_query($link,$query);  
   
   $query = "INSERT INTO workdata (cabinet, data_type, data_values)  VALUES ('".$cabinet."','date_rk_to','".$de."')";
   $a = mysqli_query($link,$query);  
   
   
   // устанавливаем флаг о том, что рк не обновлены
   
   $query = "INSERT INTO workdata (cabinet, data_type, data_values)  VALUES ('".$cabinet."','all_rk_load','false')";
   $a = mysqli_query($link,$query);  
   
   
   // устанавливаем дату обновления РК - сегодня
   
   $query = "INSERT INTO  workdata (cabinet, data_type, data_values) VALUES ('".$cabinet."', 'rekl_data_refresh', '".$dn."')";
   $a = mysqli_query($link,$query);
   
   
   
   // вызываем обновления листа РК
    
   get_rk_list(); 
    
    
}


if ($mode == 2) {
    
    // обновление рекламы по триггеру
    
     echo "Дежурный триггер на обновление рекламы \n";
      
     $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='rekl_data_refresh')"; 
     $request = mysqli_query($link,$query);
     $arr = mysqli_fetch_assoc($request);
     $refresh_date = $arr['data_values']; 
     
     $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='all_rk_load')"; 
     $request = mysqli_query($link,$query);
     $arr = mysqli_fetch_assoc($request);
     $refresh_flag = $arr['data_values']; 
       
     echo $refresh_flag."----".$refresh_date."\n";   
     
     if($refresh_flag == "false" ) {
         
         echo "РК сегодня не все обновлены, продолжаем обновление \n";
         get_rk_stat();
         
     }
     
     
     elseif($refresh_date == $dn and $refresh_flag == "true") {
         
         exit("На сегодня все обновлено - выходим");
         
     }
     
     
    
     
     elseif($refresh_date!=$dn and $refresh_flag == "true") {
     
        echo "Наступил новый день, обновляем все за вчера \n"; 
        
        $ef = date ("Y-m-d", strtotime($dn. " - 1 day"));   // вчерашняя дата
        
        $query = "DELETE from workdata WHERE (cabinet='".$cabinet."' and data_type='all_rk_load')";
        $a = mysqli_query($link,$query); 
        
        $query = "DELETE from workdata WHERE (cabinet='".$cabinet."' and data_type='rekl_data_refresh')";
        $a = mysqli_query($link,$query); 
        
        $query = "DELETE from workdata WHERE (cabinet='".$cabinet."' and data_type='date_rk_from')";
        $a = mysqli_query($link,$query); 
        
        $query = "DELETE from workdata WHERE (cabinet='".$cabinet."' and data_type='date_rk_to')";
        $a = mysqli_query($link,$query); 
        
        
        $query = "INSERT INTO workdata (cabinet, data_type, data_values)  VALUES ('".$cabinet."','all_rk_load','false')";
        $a = mysqli_query($link,$query);

        
        $query = "INSERT INTO workdata (cabinet, data_type, data_values)  VALUES ('".$cabinet."','rekl_data_refresh','".$dn."')";
        $a = mysqli_query($link,$query);   
        
        
        $query = "INSERT INTO workdata (cabinet, data_type, data_values)  VALUES ('".$cabinet."','date_rk_from','".$ef."')";
        $a = mysqli_query($link,$query);    
        
        $query = "INSERT INTO workdata (cabinet, data_type, data_values)  VALUES ('".$cabinet."','date_rk_to','".$ef."')";
        $a = mysqli_query($link,$query); 
        
        
        get_rk_list ();
     
     
     } else {exit("Нештатная ситуация");}
     
     
}








function token_take($n){
    
    global $link, $cabinet;
    
    
    if($n<5)    {
        
             // сначала проверяем есть ли токен уже в базе и не прошел ли его срок действия
            
            $query = "SELECT * FROM token_rekl WHERE (cabinet='".$cabinet."')";
            $request = mysqli_query($link,$query);
            $arr = mysqli_fetch_assoc($request);
            
            if(count($arr)>0) {             // если не пустой ответ то проверяем срок действия токена 
            
                    
                                $endtime = $arr['endtime'];         //срок действия токена, который в базе находится
                                
                                $timenow = time();                  //текущее время
                                
                                if(($endtime-$timenow)>300) {       // проверяем срок действия токена, если осталость более 5 минут - работаем по нему дальше
                                    
                                            $token = $arr['token']; // берем токен и возвращаем его
                                            
                                            
                                            
                                            return  $token;
                                            
                                    
                                } else {                            // срок действия токена менее 5 минут - выпускаем заново
                                    
                                            create_token();             // вызываем фунцию создания и записи токена
                                            sleep(1);                   // ждем секунду и заново вызваем саму себя
                                            $n++;                       // добавляем кол-во попыток и вызываем проверку заново
                
                
                                            token_take($n);             // вызываем себя и заново проверяем
                                    
                                    
                                }
                    
              
            } else {                        // токена не было, создаем новый
                
                create_token();             // вызываем фунцию создания и записи токена
                sleep(1);                   // ждем секунду и заново вызваем саму себя
                $n++;                       // добавляем кол-во попыток и вызываем проверку заново
                
              
                
                token_take($n);             // вызываем себя и заново проверяем
      
                
            }
    
    
    }
    
    
    
    
    
}

                                        

function create_token(){                        //выпуск токена
    
    global $link, $cabinet, $advert_client_id, $client_secret;
    
    $url = "https://api-performance.ozon.ru/api/client/token";
    
    $headers = [
        'Content-Type: application/json',
        'Accept:  application/json'
        ]; 
        
    
    $payload = json_encode([ 
        
        'client_id' => $advert_client_id,
        'client_secret' => $client_secret,
        'grant_type' => 'client_credentials'
        
         ]);
         
  
    $ch = curl_init();   

    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
    curl_setopt($ch, CURLOPT_POSTFIELDS, $payload);
    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
    
    $response = curl_exec($ch);
    
    $data = json_decode($response, true);
    
   
    
    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE); 
    
    if ($status==200){ 
    
    $access_token = $data['access_token'];
    $expires_in = $data['expires_in'];
    
    }
    
    if ( $access_token=='') { exit();}
    
    $time_create_token = time();
    
    $time_end_token =  $time_create_token + 1800;
    
                                                        // удаление предыдущих данных по токену в базе по этому кабинету 
    
    $query = "DELETE from token_rekl WHERE (cabinet='".$cabinet."')";
    $a = mysqli_query($link,$query);
    
                                                        //запись данных по кабинету
    
    $query = "INSERT INTO token_rekl (cabinet, token, endtime)  VALUES ('".$cabinet."', '".$access_token."', '".$time_end_token."')"; 
              
    $a = mysqli_query($link,$query);
    
 
    
}





function get_rk_list () {
    
     global $link, $cabinet;
     
     
    $query = "DELETE from list_rk  WHERE (cabinet='".$cabinet."')";
    $a = mysqli_query($link, $query);
                    
     
   
     
    // три попытки получить токен, если он пустой
     
     for ($i=1; $i<=3; $i++) {
         
        
         
         $token = token_take(0); 
         
         if($token=="") {
             
                                // Ждем 3 секунды и новая попытка
            sleep(3); 
             
             
         } else {               // Тоекн готов - выходим из цикла
             
             break;
         }
      
         
     }
     
     

     
     
     
     
     $url="https://api-performance.ozon.ru:443/api/client/campaign";
     
     $headers = [
        'Content-Type: application/json',
        'Authorization: Bearer '.$token.''
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
         
            if(count($array)>0){
         
                    
                 
                    $list=$array['list'];
                    
                   
                    foreach ($list As $elem) {
                        
                        $state = $elem['state'];
                        
                        if ($state == 'CAMPAIGN_STATE_RUNNING' || $state == 'CAMPAIGN_STATE_PLANNED' || $state == 'CAMPAIGN_STATE_STOPPED' || $state == 'CAMPAIGN_STATE_INACTIVE'){
                                   
                             // записываем только выбранные статусы
                            
                             $idc = $elem['id'];
                             $title = $elem['title'];
                             $fromDate = $elem['fromDate'];
                             $toDate = $elem['toDate'];
                             $advObjectType = $elem['advObjectType'];
                             
                             $refresh = "false";
                            
                             
                             $query = "INSERT INTO list_rk (cabinet, idc, title, fromDate, toDate, advObjectType, state, refresh)  VALUES ('".$cabinet."','".$idc."','".$title."', '".$fromDate."', '".$toDate."', '".$advObjectType."', '".$state."', '".$refresh."')";
                
                             $a = mysqli_query($link,$query);
                             
                             
                             
                             
                           
                             
                             
                            
                         
                            
                        }
                        
                        
                         
                        
                    }
                    
                       
                                                 
                        
         
            }

    }
    
     
    
}


// загрузчик рекламы основной




function get_rk_stat(){
    
     global $link, $cabinet;
     
     
     ///////////// проверяем есть ли отчет в базе
     
     echo "\n ".$cabinet."\n ";
     
     $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='rekl_id_report')"; 
     $request = mysqli_query($link,$query);
     $arr = mysqli_fetch_assoc($request);
                 
     $rekl_id_report = $arr['data_values'];
     
     $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='try_rekl_count')"; 
     $request = mysqli_query($link,$query);
     $arr = mysqli_fetch_assoc($request);
                 
     $try_report = $arr['data_values'];
     
    
     if ($rekl_id_report) {         // отчет есть
        
           echo "Отчет есть \n";
           
           echo "Количество попыток получения отчета ".$try_report."\n";
           
           if($try_report==0) {
               
                $query = "DELETE from workdata WHERE cabinet='".$cabinet."' and data_type='rekl_id_report'";
                                      
                $a = mysqli_query($link,$query); 
                
                $query = "DELETE from workdata WHERE cabinet='".$cabinet."' and data_type='try_rekl_count'";
                                      
                $a = mysqli_query($link,$query); 
               
               
               exit("Превышено количество попыток получения отчета. Генерируем снова \n");
               
           }
           
           
        
           echo "\n Заправшиваем статус отчета \n"; 
                     
                     for ($i=1; $i<=3; $i++) {
             
                         $token = token_take(0); 
                        
                         if($token=="") {
                     
                                                // Ждем 3 секунды и новая попытка
                            sleep(3); 
                       } else {               // Тоекн готов - выходим из цикла
                             
                             break;
                         }
                       }
                     
                    
                     
                     $url="https://api-performance.ozon.ru:443/api/client/statistics/".$rekl_id_report.""; //проверяем статус отчета
            
                     $headers = [
                    'Content-Type: application/json',
                    'Authorization: Bearer '.$token.''
                    
                    ]; 
                    
                    
                     $ch = curl_init();   
                 
                    curl_setopt($ch, CURLOPT_URL, $url);
                    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
                    curl_setopt($ch, CURLINFO_HTTP_CODE, true);
                    curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
                    
                    $response = curl_exec($ch);
                    $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE);    
                    
                    $try_report = $try_report-1;
                    
                    $query = "UPDATE workdata SET data_values='".$try_report."' WHERE cabinet='".$cabinet."' and data_type='try_rekl_count'";
                    
                 
                                                     
                    $a = mysqli_query($link,$query);
                    
            
                    
                    if ($status == 200) {
                        
                        $data = json_decode($response, true);
                        
                        $state = $data['state'];
                        
                        echo "\n Статус отчета".$state."\n";
                        
                        
                        
                    }
                    
                    
                    
                    if($state=="OK"){                               // если готов отчет   
        
                             echo "\n Отчет готов, грузим \n"; 
        
                            $url="https://api-performance.ozon.ru:443/api/client/statistics/report?UUID=".$rekl_id_report."";
                             
                            $ch = curl_init();   
                 
               
                 
                 
                            curl_setopt($ch, CURLOPT_URL, $url);
                            curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                            curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
                            curl_setopt($ch, CURLINFO_HTTP_CODE, true);
                            curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
                                                
                            $response = curl_exec($ch);
                                                
                            $status2 =  curl_getinfo($ch, CURLINFO_HTTP_CODE); 
                                    
                            if ($status2 != 200) {
                        
                                 exit();   
                                                  
                            }
                        
                            
                            //запрашиваем дату с которой у нас был запрос на кампанию
                            
                             $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='date_rk_from')"; 
                             $request = mysqli_query($link,$query);
                             $arr = mysqli_fetch_assoc($request);
                             $dff = $arr['data_values']; 
                             
                             $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='date_rk_to')"; 
                             $request = mysqli_query($link,$query);
                             $arr = mysqli_fetch_assoc($request);
                             $eff = $arr['data_values']; 
                             
                            $response = json_decode($response, true);
                  
                            $ids = array_keys($response);
                  
                 
                  
                             for($i=0; $i<count($ids); $i++){              // перебираем ID РК
                      
                      
                                        $id_rk = $ids[$i];
                                        
                                         // удаляем все данные по этой рк из запрашиваемого диапазона дат по ключу кабинет + РК + Даты
                                         
                                          
                                                
                                         $query = "DELETE from stat_rk  WHERE (cabinet='".$cabinet."' and id_rk='".$id_rk."' and date >='".$dff."' and date <='".$eff."')";
                                         $a = mysqli_query($link,$query);
                                                
                                         
                                         echo "Удалены данные по кампании ".$id_rk." с ".$dff." по ".$eff.". ";
                     
                     
                                         //запрашиваем тип рекламы
                                         
                                         $query = "SELECT * FROM list_rk WHERE (cabinet='".$cabinet."'  and idc='".$id_rk."')"; 
                                         $request = mysqli_query($link,$query);
                                         $arr = mysqli_fetch_assoc($request);
                         
                                         $type_rekl = $arr['advObjectType'];   
                     
                     
                                         $rowdata = $response[$id_rk];          
                                         
                                         $title_rk =  $rowdata['title'];
                                         
                                         $rowsdata =  $rowdata['report']['rows'];
                            
                                         for ($j=0; $j<count($rowsdata); $j++){                 // перебираем массив 
                                             
                                                $rowData_r = $rowsdata[$j];                       // добавляем в базу
                                             
                                                $date = $rowData_r['date'];
                                                
                                                $date = date ("Y-m-d", strtotime($date));
                                             
                                                $sku = $rowData_r['sku'];
                                                
                                                $views = $rowData_r['views'];
                                                
                                                $clicks = $rowData_r['clicks'];
                                                
                                                $ctr = str_replace('.', ',' , $rowData_r['ctr']);
                                                
                                                $moneySpent = str_replace('.', ',' , $rowData_r['moneySpent']);
                                                
                                                $avgBid = str_replace('.', ',' , $rowData_r['avgBid']);
                                             
                                                $orders = $rowData_r['orders'];
                                                
                                                $ordersMoney = $rowData_r['ordersMoney'];
                                                
                                                $models = $rowData_r['models'];
                                                
                                                $modelsMoney = str_replace('.', ',' , $rowData_r['modelsMoney']);
                                                
                                                $title_t = $rowData_r['title'];
                                                
                                                $price = str_replace('.', ',' , $rowData_r['price']); 
                                                
                                                $toCart = $rowData_r['toCart'];
                                                
                                                
                                                $title_t = str_replace('\'','\"',  $title_t); 
                                                
                                                // запись данных
                                                
                                                $query = "INSERT INTO stat_rk (cabinet, id_rk, type_rekl, title_rk, date, sku, views, clicks, ctr, moneySpent, avgBid, orders, ordersMoney, models, modelsMoney, title_t, price, toCart) VALUES ('".$cabinet."', '".$id_rk."', '".$type_rekl."', '".$title_rk."', '".$date."', '".$sku."', '".$views."', '".$clicks."', '".$ctr."', '".$moneySpent."', '".$avgBid."', '".$orders."', '".$ordersMoney."', '".$models."', '".$modelsMoney."', '".$title_t."', '".$price."', '".$toCart."')";
                                                
                                                $a = mysqli_query($link,$query);
                            
                                                
                                         }
                        
                                            // обновление статуса рк в таблице
                                            
                                            $query = "UPDATE list_rk SET refresh='true' WHERE idc ='".$id_rk."'";
                                                     
                                            $a = mysqli_query($link,$query);
                                            
                                            echo "\n RK state true ".$id_rk;
                             
                                }
                  
                                          //удаляем id отчета, который запрашивали и счетчик удаяем
                                            
                                          $query = "DELETE from workdata WHERE cabinet='".$cabinet."' and data_type='rekl_id_report'";
                                          
                                          $a = mysqli_query($link,$query); 
                                          
                                          $query = "DELETE from workdata WHERE cabinet='".$cabinet."' and data_type='try_rekl_count'";
                                      
                                          $a = mysqli_query($link,$query); 
                    
                    } 
                     
                    
                    
       
       
        
        
     }
     
     
     else {                      // отчета нет
         
                
                echo "Отчета нет \n";
        
        
                $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='date_rk_from')"; 
                $request = mysqli_query($link,$query);
                 
                $arr = mysqli_fetch_assoc($request);
                $df = $arr['data_values'];
                 
                $query = "SELECT * FROM workdata WHERE (cabinet='".$cabinet."' and data_type='date_rk_to')"; 
                $request = mysqli_query($link,$query);
                 
                $arr = mysqli_fetch_assoc($request);
                $de = $arr['data_values'];
                 
                if(!$df and !$de) { exit("Даты для обновлений не заданы в базе - выходим!");}
                 
                 
                $idBlock=[];     // массив для ID РК
         
         
                $query = "SELECT * FROM list_rk WHERE (cabinet='".$cabinet."')"; 
                    
                $request = mysqli_query($link,$query);
                    
                $adverts_array = mysqli_fetch_all($request);
                   
                $id_count = 0;
                $first_id = -1;
            
                for ($i=0; $i<count($adverts_array); $i++) {
                                
                                   
                                    
                                    if($adverts_array[$i][8] == 'false' && $id_count < 10) {
                                        
                                          array_push($idBlock, $adverts_array[$i][2]);    
                                           
                                         
                                          // фиксируем первый элемент массива
                                          
                                          if ($first_id == -1) {
                                              
                                              $first_id = $i;
                                            
                                          }
                                          
                                           $id_count++;
                                   
                                    }  
                                    
                              
                                    
                                    if($id_count>10) {
                                        break;
                                    }
                                   
                                
                }
                        
         
            
                        //если нечего обновлять - выходим. - это основной сброс функции
                        
                if ($first_id == -1) {
                            
                            
                            $query = "DELETE from workdata  WHERE (cabinet='".$cabinet."' and data_type='date_rk_from')";
                            $request = mysqli_query($link,$query);
                            
                            $query = "DELETE from workdata  WHERE (cabinet='".$cabinet."' and data_type='date_rk_to')";
                            $request = mysqli_query($link,$query);
                            
                            
                            $query = "DELETE from workdata  WHERE (cabinet='".$cabinet."' and data_type='all_rk_load')";
                            $a = mysqli_query($link,$query);
            
                            $query = "INSERT INTO  workdata (cabinet, data_type, data_values) VALUES ('".$cabinet."', 'all_rk_load', 'true')";
                            $a = mysqli_query($link,$query);
                            
                            
                            
                            //И тут обновляем на вчера дату from для отчета
                            
                            exit("РК все обновлены");
                            
                }
                
                
                
                
                
                 // запрашиваем токен
                 
                 for ($i=1; $i<=3; $i++) {
             
                         $token = token_take(0); 
                        
                         if($token=="") {
                     
                                                // Ждем 3 секунды и новая попытка
                            sleep(3); 
                       } else {               // Тоекн готов - выходим из цикла
                             
                             break;
                         }
                }   
                
                  
                  $url = "https://api-performance.ozon.ru:443/api/client/statistics/json"; 
        
                        $headers = [
                                'Content-Type: application/json',
                                'Authorization: Bearer '.$token.''
                                ]; 
            
            
                        $payload = [ 
                                
                                "campaigns" => $idBlock,
                                "dateFrom" => $df,
                                "dateTo" => $de,
                                "groupBy" => "DATE"
                            
                        ];
                        
                            
                         echo "\n Грузим блок кампаний \n";
                        
                         print_r ($idBlock);
     
        
                         $payload = json_encode($payload);
        
        
        
      
            
                        $ch = curl_init();   
        
                        curl_setopt($ch, CURLOPT_URL, $url);
                        curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                        curl_setopt($ch, CURLOPT_CUSTOMREQUEST, 'POST');
                        curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, false);
                        curl_setopt($ch, CURLOPT_POSTFIELDS, $payload);
                        curl_setopt($ch, CURLINFO_HTTP_CODE, true);
                        curl_setopt($ch, CURLOPT_HTTPHEADER, $headers);
                        
                        $response = curl_exec($ch);
                    
                        $status =  curl_getinfo($ch, CURLINFO_HTTP_CODE); 
                        
                        echo $response;
                        
                        if ($status == 200) {
                             
                             
                             $response = json_decode($response, true);
                        
                             $UUID = $response['UUID'];                                 //отправли задачу на генерацию отчета и получили ID отчета
                          
                       
                          
                        } else {exit("Отчет не получен - пробуем еще \n");}
         
        
        
                        if ($UUID) {
                        
                        echo "\n отчет запрошен - ".$UUID;
                        
                        $query = "DELETE from workdata  WHERE (cabinet='".$cabinet."' and data_type='rekl_id_report')";
                        $a = mysqli_query($link,$query);
                        
                        $query = "INSERT INTO  workdata (cabinet, data_type, data_values) VALUES ('".$cabinet."', 'rekl_id_report', '".$UUID."')";
                        $a = mysqli_query($link,$query);
                        
                        $query = "INSERT INTO  workdata (cabinet, data_type, data_values) VALUES ('".$cabinet."', 'try_rekl_count', 7)";
                        $a = mysqli_query($link,$query);
                        
                        exit(" отчет записан, выходим \n"); 
                        
                        }
                        
              
         
         
        
         
         
         
     }  
    
    
        
}        
        
        


                     
      
                                            
            
     
    
    
?>