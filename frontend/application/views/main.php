<h2 class="section">
	Search
</h2>
<form name="form" method="post" action="<?= base_url();?>index.php/main/search">
    <div class="section-content">
		<div class="contenedorCajita">
			Query: <input name="nombre" id="nombre" type="text" value="<?php //echo $nombre?>" /> 
            <input type="hidden" value="<?php echo $user_id?>" name="user_id" class="button" />
			<input class="btn btn-primary btn-large" type="submit" value="Filtrar" name="" class="button" />
        </div>
		<input type="hidden" name="page" id="page" value="<?php //echo $page?>" />
	</div>
</form>