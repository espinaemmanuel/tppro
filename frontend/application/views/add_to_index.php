<?php $this->load->view('include/header');?>
<?php $this->load->view('menu');?>

<div class="container-fluid">
  
  <br><h3>Add to index</h3>  
  <input id="file_upload" name="file_upload" type="file" />
  <div class="indexing" style="display: none;">Indexing...</div>
  
</div>  
    
<?php $this->load->view('include/footer');?>

<script src="<?=base_url('assets/js/jquery.min.js')?>"></script>
<script src="<?=base_url('assets/js/bootstrap.min.js')?>"></script>
<script src="<?=base_url('uploadify/jquery.uploadify.min.js')?>"></script>

<script type="text/javascript">
    
$(function() {
    $('#file_upload').uploadify({
        'swf'      : '<?=base_url()?>uploadify/uploadify.swf',
        'uploader' : '<?=base_url()?>uploadify/uploadify.php',
        // Your options here
        'onUploadSuccess' : function() {},
        'onQueueComplete': function() {
            //console.log('listo');
            indexAll(<?= $this->session->userdata('user_id') ?>);
            $('.indexing').show();
        },
        'onError' : function () {},
        'method':'get',
		'formData' : { 'user_id' : '<?= $this->session->userdata('user_id') ?>' }
    });
});

function indexAll(user_id) {
    console.log('indexa todos los arch del usuario', user_id);
    $.get('<?=base_url()?>main/make_index/'+user_id, function(){
      $('.indexing').hide();;
    } );
}    
</script>  