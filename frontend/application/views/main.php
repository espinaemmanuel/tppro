<?php $this->load->view('include/header');?>
<?php $this->load->view('menu');?>

<div class="container-fluid">
  
  <br><h2>Search</h2>  
  
  <form name="form" method="post" action="<?= base_url();?>index.php/main/search">
    <input name="query" id="query" type="text" value="<?php echo $query?>" /> 
    <input type="hidden" value="<?php echo $user_id?>" name="user_id" class="button" />
    <input class="btn btn-primary btn-large" type="submit" value="Filtrar" name="" class="button" />

  </form>

    <table class="table">
        <tbody>
            <?php if(isset($result->hits)){ ?>
            <tr class="list_header">
                <th style="width:800px;">Results</th>
            </tr>
            <?php
              foreach ($result->hits as $hit) {
              echo "<tr>";  
                echo "<th>".$hit->doc->fields->text."</th>";
              echo "</tr>";  
              }
            }?>
        </tbody>
    </table>
  </div>
    
<?php $this->load->view('include/footer');?>

  <!-- Le javascript==================================================-
  ->
  <!-- Placed at the end of the document so the pages load faster -->
  <script src="//ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min.js"></script>
  <script src="//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.0/js/bootstrap.min.js"></script>