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
            for ($j=0; $j<count(reset($mirrors)); $j++) {
              echo "<tr>";
                echo "<th>$i</th>";
                foreach ($partitions as $part){ 
                  echo "<th id='status_$part"."_$i' class='".$mirrors[$part][$i-1]->status."'></th>";
                }  
              echo "</tr>";  
            $i++;
          }?>
      </tbody>
  </table>
</div>
    
<?php $this->load->view('include/footer');?>

<script src="//ajax.googleapis.com/ajax/libs/jquery/1.8.2/jquery.min.js"></script>
<script src="//netdna.bootstrapcdn.com/twitter-bootstrap/2.3.0/js/bootstrap.min.js"></script>
  
<script type="text/javascript">
  
  setInterval(function() {
    $.ajax({
        url: 'http://localhost/tppro/phpClient/getStatus.php',
        type: 'GET',
        dataType: 'json',
        data: 'extraparam=45869159&another=32',
        success: function (data) {
            //console.log(data[1][0]);

            $('#status_1_1').removeClass('active');
            $('#status_1_1').addClass(data[1][0].status);
            
            $('#status_1_3').removeClass('active');
            $('#status_1_3').addClass(data[1][2].status);
        }
    });
  }, 5000);
</script>