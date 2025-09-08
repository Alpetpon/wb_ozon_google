<?php



include 'sql.php'; //загрузка параметров MySQL


$data = json_decode(file_get_contents('php://input'), true);

$user_id = $data[user_id];     
     
$mp = $data[mp];   


if ($mp == "wb") {

$query = "SELECT  * from bot_apikey_wb  WHERE user_id='".$user_id."'";	
$a = mysqli_query($link,$query);
$n = mysqli_num_rows($a);


	if ($n>0) { echo 1; die;}
	else {echo 0; die;}

	
} 

if ($mp == "ozon") {

$query = "SELECT  * from bot_apikey_ozon  WHERE user_id='".$user_id."'";	
$a = mysqli_query($link,$query);
$n = mysqli_num_rows($a);


	if ($n>1) { echo 1; die;}
	else {echo 0; die;}

	
} 

if ($mp == "wbozon") {

$query = "SELECT  * from bot_apikey_wb  WHERE user_id='".$user_id."'";	
$a = mysqli_query($link,$query);
$n = mysqli_num_rows($a);


$query2 = "SELECT  * from bot_apikey_ozon  WHERE user_id='".$user_id."'";	
$a2 = mysqli_query($link,$query2);
$n2 = mysqli_num_rows($a2);


$n3 = $n+$n2;


	if ($n3>2) { echo 1; die;}
	else {echo 0; die;}

	
} 




?>