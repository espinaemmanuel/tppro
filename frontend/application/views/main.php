<h2 class="section">
	Search
</h2>
<form name="form" method="post" action="<?= base_url();?>index.php/main/search">
    <div class="section-content">
		<div class="contenedorCajita">
			<input name="query" id="query" type="text" value="<?php echo $query?>" /> 
            <input type="hidden" value="<?php echo $user_id?>" name="user_id" class="button" />
			<input class="btn btn-primary btn-large" type="submit" value="Filtrar" name="" class="button" />
        </div>
        <div class="result">
          <?php if(isset($result->hits)){ ?>
            <h3 class="section">
                Result
            </h3>
          <?php
            foreach ($result->hits as $hit) {
              print "<p>".$hit->doc->fields->text."</p>";
            }
          }
          ?>
        </div>
		<input type="hidden" name="page" id="page" value="<?php //echo $page?>" />
	</div>
</form>