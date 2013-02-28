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
        'onUploadSuccess' : function() {},
        'onQueueComplete': function() {
            console.log('listo');
            indexAll(<?= $this->session->userdata('user_id') ?>);
        },
        'onError' : function () {},
        'method':'get',
		'formData' : { 'user_id' : '<?= $this->session->userdata('user_id') ?>' }
    });
});

function indexAll(user_id) {
    console.log('indexa todos los arch del usuario', user_id);
    $.get('/tppro/phpClient/indexService.php', { user_id: user_id, partitions: 2 }, function(){alert('ahhh');} );
}
</script>  