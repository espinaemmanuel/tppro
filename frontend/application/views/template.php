<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="es" xml:lang="es">
<head>
    <title></title>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
<meta name="author" content="Carxl" />
<meta name="robots" content="All" />
<script type="text/javascript" src="<?php echo site_url('uploadify/jquery-1.4.2.min.js')?>"></script>
<script type="text/javascript" src="<?php echo site_url('uploadify/jquery.uploadify.v2.1.4.min.js');?>"></script>
<script type="text/javascript" src="<?php echo site_url('uploadify/swfobject.js');?>"></script>
<link href="<?php echo site_url('uploadify/uploadify.css');?>" type="text/css" rel="stylesheet" />
<script type="text/javascript">
$(document).ready(function() {
    var texto = '';
	$("#fileUpload1").uploadify({
        'uploader'    : '<?php echo site_url('uploadify/uploadify.swf');?>',
        'cancelImg'   : '<?php echo site_url('uploadify/cancel.png');?>',
		'script'      : '<?php echo $rutaAbsolutaSubir;?>',
        'folder'      : '',
		'multi'       : true,
        'auto'        : true,
		'buttonText'  : 'Buscar',
		'displayData' : 'speed',
		'simUploadLimit': 2
	});
});
</script>
</head>
<body>
	<h1>Ejemplo de como implementar Uploadify en Codeigniter</h1>
    <div id="fileUpload1">You have a problem with your javascript</div>
</body>
</html>