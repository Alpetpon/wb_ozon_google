<?php

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$user_id = $data['user_id'];    

$tab = $data['tab'];  

$dF = $data['dF']; 

$dT = $data['dT']; 
     


 // заправшиваем ключи из базы по идентификатору клиента

$query = "SELECT * from bot_apikey_wb  WHERE (user_id='".$user_id."')";



$a = mysqli_query($link,$query);

$result = [];

while($row = mysqli_fetch_assoc($a)) {
	
	
	$cabinet = $row['cabinet'];
	
	// выгрузка из базы данных
	
	
	if ($tab=="detaliz_wb") {
		
		$query = "SELECT * from ".$tab." WHERE (cabinet='".$cabinet."' and rr_dt >= '".$dF."'  and rr_dt <= '".$dT."')";
		
	} else {
		
		$query = "SELECT * from ".$tab." WHERE (cabinet='".$cabinet."' and dF >= '".$dF."'  and dF <= '".$dT."')";
		
	}
	
	
	$b = mysqli_query($link,$query);
	
	
	
	
	while($rows = mysqli_fetch_assoc($b)) {
    $result[] = $rows;
	
	}
	
  
   
}

	echo json_encode($result);






?>