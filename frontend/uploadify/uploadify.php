<?php

/*
 Uploadify
Copyright (c) 2012 Reactive Apps, Ronnie Garcia
Released under the MIT License <http://www.opensource.org/licenses/mit-license.php>
*/

// Define a destination
$targetFolder = '/tppro/frontend/uploads/'; // Relative to the root
$verifyToken = md5('unique_salt' . rand() );

if (!empty($_FILES) ) {
  
    $tempFile = $_FILES['Filedata']['tmp_name'];
	$targetPath = $_SERVER['DOCUMENT_ROOT'] . $targetFolder;
	
	$targetFile = rtrim($targetPath,'/') ."/". $_GET['user_id']."_".$verifyToken.".txt";

	// Validate the file type
	//$fileTypes = array('jpg','jpeg','gif','png'); // File extensions
	//$fileParts = pathinfo($_FILES['Filedata']['name']);

	//if (in_array($fileParts['extension'], $fileTypes)) {
		move_uploaded_file($tempFile, $targetFile);
	//	echo '1';
	//} else {
	//	echo 'Tipo de archivo invalido.';
	//}
}
?>