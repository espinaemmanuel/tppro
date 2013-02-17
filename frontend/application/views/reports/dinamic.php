<?php $this->load->view('include/header');?>
<?php $this->load->view('menu');?>

<div class="container-fluid">
  
  <br><h2>Report</h2>  
  
  <table class="table">
      <tbody>
          <h3>Partitions</h3>
          <tr>
            <th>Mirrors</th>
            <?php foreach ($partitions as $part){ ?>
                <th><?= $part->partition_id ?></th>
            <?php }?>
          </tr>
          <?php
            foreach ($mirrors as $mirror) {
            echo "<tr>";
              echo "<th>".$mirror."</th>";
              echo "<th>".$mirror."</th>";
              echo "<th>".$mirror."</th>";
              echo "<th>".$mirror."</th>";
            echo "</tr>";  
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