<?php

include 'sql.php'; //загрузка параметров MySQL

// блок получения айди клиента и апи ключа из входящего POST JSON

$data = json_decode(file_get_contents('php://input'), true);

$user_id = $data['user_id'];    

$tab = $data['tab'];  

$df = $data['dF']; 

$de = $data['dT']; 
     


 // заправшиваем ключи из базы по идентификатору клиента

$query = "SELECT * from bot_apikey_ozon  WHERE (user_id='".$user_id."')";



$a = mysqli_query($link,$query);

$result = [];

while($row = mysqli_fetch_assoc($a)) {
	
	
	$cabinet = $row['cabinet'];
	
	// выгрузка из базы данных

	if ($table == 'tovar'){

	$query = "SELECT * from tovar WHERE cabinet='".$cabinet."'";

	}
	
	
	if ($table == 'detaliz'){       // даты





	$query = "SELECT * from detaliz WHERE (cabinet='".$cabinet."' and c1 >='".$df."'  and c1 <='".$de."')";
	
	}
	
	if ($table == 'prices'){       // даты

	$query = "SELECT * from prices WHERE (cabinet='".$cabinet."' and dat >='".$df."'  and dat <='".$de."')";
	
	}
	
	if ($table == 'stat_rk'){       // даты

	$query = "SELECT * from stat_rk WHERE (cabinet='".$cabinet."' and date >='".$df."'  and date <='".$de."')";
	
	}
	
	if ($table == 'voronka'){       // даты

		$query = "SELECT * from voronka WHERE (cabinet='".$cabinet."' and dat >='".$df."'  and dat <='".$de."')";
	
	}
	
	if ($table == 'stocks'){       // даты

	$query = "SELECT * from stocks WHERE (cabinet='".$cabinet."' and dat >='".$df."'  and dat <='".$de."')";
	
	}
	
	if ($table == 'report_fbo'){       // даты

	$query = "SELECT * from report_fbo WHERE (cabinet='".$cabinet."' and created_at >='".$df."'  and created_at <='".$de."')";
	
	}
	
	if ($table == 'report_fbs'){       // даты

	$query = "SELECT * from report_fbs WHERE (cabinet='".$cabinet."' and in_process_at >='".$df."'  and in_process_at <='".$de."')";
	
	}
	
	if ($table == 'zakaz_fbo'){       // даты

	$query = "SELECT * from zakaz_fbo WHERE (cabinet='".$cabinet."' and c2 >='".$df."'  and c2 <='".$de."')";
	
	}
	
	if ($table == 'zakaz_fbs'){       // даты

	$query = "SELECT * from zakaz_fbs WHERE (cabinet='".$cabinet."' and c2 >='".$df."'  and c2 <='".$de."')";

	}
	
	
	
	$b = mysqli_query($link,$query);
	
	
	
	
	while($rows = mysqli_fetch_assoc($b)) {
    $result[] = $rows;
	
	}
	
  
   
}

	echo json_encode($result);






?>