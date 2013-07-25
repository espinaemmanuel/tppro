<?php $this->load->view('include/header');?>
<?php $this->load->view('menu');

/*Data una direccion, la parsea y devuelve el puerto. ip:puerto y ip_puerto*/
function getPort($address){
  if(strpos($address, '_')>-1)
    return substr($address, strpos($address, '_')+1);
  return substr($address, strpos($address, ':')+1);
}

/*
 * Se guarda un array con las particiones tanto en PHP ($parts) como en JS (parts)
 */
$parts=array();?>
<script type="text/javascript">
var parts=new Array();
<?php
foreach ($replicas as $replica) { 
  foreach ($replica as $data) {
   $parts["'".$data['groupId']."_".$data['partitionId']."'"]=$data;?>
   parts['<?php echo $data['groupId']."_".$data['partitionId']?>']=new Array(<?php echo $data['groupId'].','.$data['partitionId']?>);
   <?php
  }
}
?>
</script>  

<div class="container-fluid">
  <?php if(is_array($nodes)){ ?>
  
  <br><h3 style="padding-left: 0px;">Nodes and replicas status</h3>
  
  <table id="table" class="table">
      <tbody>
          <tr>
            <th>Replicas</th>
            <?php 
            foreach ($nodes as $node){ ?>
                <th><?= $node ?></th>
            <?php }?>
          </tr>
          <?php
            foreach ($parts as $part)  {?>
              <tr>
                <th  style='width: 130px;'>Group id: <?=$part['groupId']."<br>Partition id: ".$part['partitionId'];?></th> 
                <?php 
                /*
                 * Armo la fila: nodo 1 | nodo 2 | .... | nodo n  
                 */
                foreach ($nodes as $node){
                  //echo "<pre>";
                  //print_r($nodes);
                  //echo "</pre>------";
                  if(isset($replicas[$node])){
                    foreach($replicas[$node] as $data){
                      $node_replica=NULL;
                      if($data['groupId']==$part['groupId'] && $data['partitionId']===$part['partitionId']){
                        $node_replica=$data;
                        break;
                      }
                    }
                  }
                  else{
                    $node_replica['status']="";
                  }
                  /* Armo las columnas: 
                   * Group id: 1 Replica: 1
                   * Group id: 1 Replica: 2
                   */
                  ?>
                  <th class="<?=$node_replica['status'];?>">
                    <?php if($node_replica!=NULL){?>
                      <a class="tooltip2" href="#">&nbsp;&nbsp;&nbsp;&nbsp;</a>
                      <div class="tooltip-seg">
                        Group id: <?=$node_replica['groupId'];?><br>
                        Partition id: <?=$node_replica['partitionId'];?><br>
                        Status: <?=$node_replica['status'];?>
                      </div><br/>
                    <?php }?>
                  </th>
                <?php }  
              echo "</tr>";  
          }?>
      </tbody>
  </table>
  
  <?php } 
  
  if(is_array($groupVersion)){ ?>

  <div class="container-fluid">
  <br><h3>Groups info</h3>  
  <table class="table">
        <tbody>
            <tr class="list_header">
                <th style="width:100px;">Group Id</th>
                <th style="width:100px;">Version</th>
            </tr>
            <?php 
            foreach ($groupVersion as $group=>$version) {?>
                <tr>  
                  <th><?=$group;?></th>
                  <th><?=$version;?></th>
                </tr>  
            <?php }?>
        </tbody>
    </table>
  </div>

  <?php } ?>

</div>

<?php $this->load->view('include/footer');?>

<script src="<?=base_url()?>/assets/js/jquery.min.js"></script>
<script src="<?=base_url()?>/assets/js/bootstrap.min.js"></script>
  
<script type="text/javascript">
setInterval(function() {
  $.ajax({
      url: '<?php echo URL_MONITOR?>',
      type: 'GET',
      dataType: 'json',
      data: 'extraparam=45869159&another=32',
      success: function (data) {
          var estado = '<tbody>'+
          '<tr>'+
            '<th>Replicas</th>';

          for(var i in data['nodes'])  
            estado+='<th>'+ data['nodes'][i] +'</th>';
          
          for (var j in parts) {
              estado+='<tr>'+
                '<th  style="width: 130px;">Group id: '+parts[j][0]+'<br>Partition id: '+parts[j][1]+'</th>'; 
              for(var z in data['nodes']){
                var node=data['nodes'][z];
                var node_replica='';
                for (var k in data['replicas'][node]){
                    node_replica='';
                    if(data['replicas'][node][k]['groupId']==parts[j][0] && data['replicas'][node][k]['partitionId']==parts[j][1]){
                      node_replica=data['replicas'][node][k];
                      break;
                    }
                  }
                if(node_replica!=''){
                  estado+='<th class="' + node_replica['status'] + '">'+
                  '<a class="tooltip2" href="#">&nbsp;&nbsp;&nbsp;&nbsp;</a>'+
                  '<div class="tooltip-seg">'+
                  'Group id: ' + node_replica['groupId'] + '<br>'+
                  'Partition id: ' + node_replica['partitionId'] + '<br>'+
                  'Status: ' + node_replica['status']+
                  '</div><br/>';
                }
                else{
                  estado+='<th>';
                }
                estado+='</th>';
                }  
              estado+='</tr>';
          }
          estado+='</tbody>';
          //console.log("estado3",estado);
          $('#table').html(estado);
      }
  });
}, 1000);
</script>
<script type="text/javascript">
$(document).ready(function() {
  $(document).on("mouseover", ".tooltip2", function(){
      $(".tooltip2").mousemove(function(e){
           $(this).next().css({left : e.pageX , top: e.pageY});
        });
      eleOffset = $(this).offset();
      $(this).next().fadeIn("fast").css({

              left: eleOffset.left + $(this).outerWidth(),
              top: eleOffset.top

          });
  }).on("mouseout", ".tooltip2", function(){
        $(this).next().fadeOut("slow");
    }); 
});

</script>