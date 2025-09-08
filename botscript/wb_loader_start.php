<?php

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$user_id = $data['user_id'];    

$file = $data['file'];       

$dF = $data['dF']; 

$dT = $data['dT']; 



 // заправшиваем ключи из базы по идентификатору клиента

$query = "SELECT * from bot_apikey_wb  WHERE (user_id='".$user_id."')";
$a = mysqli_query($link,$query);

while($row = mysqli_fetch_assoc($a)) {
	
	
	$cabinet = $row['cabinet'];
	$apiKey = $row['apikey'];

	
	
	$url = "https://559b5f87f193.hosting.myjino.ru/botscript/".$file.".php";

	
  
	$body = json_encode([ "cabinet" =>  $cabinet,  "apiKey" => $apiKey,  "dF" => $dF,  "dT" => $dT]);
	
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
  
  
   
}

//echo "true";







?>