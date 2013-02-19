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
                <th><?= $part ?></th>
            <?php }?>
          </tr>
          <?php
            $i=1;
            foreach ($mirrors as $mirror) {
            echo "<tr>";
              echo "<th>$i</th>";
              echo "<th class='$mirror[1]'>".$mirror[1]."</th>";
              echo "<th class='$mirror[2]'>".$mirror[2]."</th>";
              echo "<th class='$mirror[3]'>".$mirror[3]."</th>";
              echo "<th class='$mirror[4]'>".$mirror[4]."</th>";
            echo "</tr>";  
            $i++;
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