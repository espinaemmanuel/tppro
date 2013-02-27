<?php $this->load->view('include/header');?>
<?php $this->load->view('menu');?>

<div class="container-fluid">
  
  <br><h2>Add to index</h2>  
  
  <form action="" method="post">
    <input type="file" name="file_upload" id="file_upload" />
  </form>
  
</div>  
    
<?php $this->load->view('include/footer');?>

<script src="//ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min.js"></script>
<script src="//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.0/js/bootstrap.min.js"></script>
<script src="<?=base_url()?>uploadify/jquery.uploadify.min.js"></script>

<script type="text/javascript">
    
$(function() {
    $('#file_upload').uploadify({
        'swf'      : '<?=base_url()?>uploadify/uploadify.swf',
        'uploader' : '<?=base_url()?>uploadify/uploadify.php',
        // Your options here
        'onUploadSuccess' : function() {
            //console.log('listo un upload');
        },
        'onQueueComplete': function() {
            console.log('listo');
            indexAll(<?= $this->session->userdata('user_id') ?>);
        },
        'onError' : function (event,ID,fileObj,errorObj) {
          alert(errorObj.type + ' Error: ' + errorObj.info);
        },
        'method':'get',
		'formData' : { 'user_id' : '<?= $this->session->userdata('user_id') ?>' }
    });
});

function indexAll(user_id) {
    
    console.log('indexa todos los arch del usuario', user_id);
    
    /*var resultado = '';
    $.get('/backend/services/imagenes/tipo/event/id/< ?=$event->id?>', function (data) {
        $.each(data, function(index, value) {
            resultado += "<a href='"+value.url+"'><img src='"+value.url+"' class='imagenThumb'/></a>";
            resultado += "<a href='#' onclick='borrarImagen(\""+value.imagen+"\")' ><img class='deleteIcon' src='/backend/img/delete.png' /></a>";
        });
        $('#imagenes').html(resultado);
    }, "json");*/
}

/*$(function() {
    cargarImagenes();
});   */ 
</script>  