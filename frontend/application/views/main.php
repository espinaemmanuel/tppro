<?php $this->load->view('include/header');?>
<?php $this->load->view('menu');?>

<script type="text/javascript" src="<?= base_url();?>assets/js/jquery.min.js"></script>

<div class="container">
	<div id="content">
    
        <div id="login-box" class="login-popup" style="width: 496px;">
        <a href="#" class="close"><img src="<?=base_url();?>assets/img/close_pop.png" class="btn_close" title="Close Window" alt="Close" /></a>
			<img id="poster" src="" width="242px"/>
			<div style="float: right; width: 244px; color: #999999">
				<span id="description" >Movie description movie description movie description movie description movie description movie description</span><br><br>
                <span>Director:</span><br>
                <span id="movie-director">Quentin Tarantino</span><br><br>
                <span>Release:</span><br>
                <span id="release">12/08/1990</span>
            </div>        
     	</div>
    
    </div>
</div>

<div class="container-fluid">
  
  <br><h2>Search</h2>  
  
  <form name="form" method="post" style="width: 240px;" action="<?= base_url();?>index.php/main/search">
    <input name="title" id="title" placeholder="Title" type="text" value="<?=$title?>" /><br/>  
      <div class="btn btn-primary btn-large op" id="op">
        And
      </div><br/> 
    <input name="text" id="text" placeholder="Overview" type="text" value="<?= $text?>" /><br/> 
    <?php $style="display: none;"; $button_text="Add director"; $addDir="addDir"; 
    if($director!=""){ $style = ""; $button_text="Hide director"; $addDir="hideDir";}?>
    <div class="btn btn-primary btn-large op" id="addDir">
        <?=$button_text;?>
    </div>
    <input name="director" id="director" placeholder="Director" style="<?=$style;?>" type="text" value="<?= $director?>" /><br/>
    <?php $style="display: none;"; $button_text="Add year"; $addYear="addY"; 
    if($year!=""){ $style = ""; $button_text="Hide year"; $addYear="hideY";}?>
    <div class="btn btn-primary btn-large op" id="addY">
        <?=$button_text;?>
    </div> 
    <input name="year" id="year" placeholder="Release greater than ..." style="<?=$style;?>" type="text" value="<?= $year?>" /><br/>
    
    <?php $style="display: none;"; $button_text="Add genre"; $addGenre="addG"; 
    if($genre!=""){ $style = ""; $button_text="Hide genre"; $addGenre="hideG";}?>
    <div class="btn btn-primary btn-large op" id="addG">
        <?=$button_text;?>
    </div> 
    <input name="genre" id="genre" placeholder="Genre" style="<?=$style;?>" type="text" value="<?= $genre?>" /><br/>
 
    <input type="hidden" value="<?php echo $user_id?>" name="user_id" class="button" />
    <input type="hidden" value="and" name="operator" id="operator" class="button" />
    <input type="hidden" value="<?=$addDir;?>" name="addDirector" id="addDirector" class="button" />
    <input type="hidden" value="<?=$addYear;?>" name="addYear" id="addYear" class="button" />
    <input type="hidden" value="<?=$addGenre;?>" name="addGenre" id="addGenre" class="button" />
    <input class="btn btn-primary btn-large" type="submit" value="Search" name="" class="button" />

  </form>

    <table class="table">
        <tbody>
            <?php if(isset($result->qr->hits)){ ?>
            <tr class="list_header">
                <th style="width:150px;">Title</th>
                <th style="width:640px;">Overview</th>
                <th style="width:140px;">Director</th>
                <th style="width:100px;">Release</th>
            </tr>
        <script type="text/javascript">movies=new Array();</script>
            <?php $i=0;
              foreach ($result->qr->hits as $hit) {?>
                <script type="text/javascript">
                  movie=new Object();
                  movie.title='<?=$hit->doc->fields->title;?>';
                  movie.overview='<?=htmlspecialchars ($hit->doc->fields->overview, ENT_QUOTES);?>';
                  movie.director='<?=htmlspecialchars ($hit->doc->fields->director, ENT_QUOTES);?>';
                  movie.release='<?=$hit->doc->fields->release;?>';
                  <?php if(isset($hit->doc->fields->poster)){?>
                    movie.poster='<?=$hit->doc->fields->poster;?>';
                  <?php }?>  
                  movies[<?=$i;?>]=movie;
                </script>
                <tr onclick="showBox(<?=$i++;?>);">  
                  <th><?=$hit->doc->fields->title;?></th>
                  <th id="overview"><?=  substr($hit->doc->fields->overview,0,80)."...";?></th>
                  <th><?=  $hit->doc->fields->director;?></th>
                  <th><?=  $hit->doc->fields->release;?></th>
                  
                </tr>  
              <?php }
            }?>
        </tbody>
    </table>
  </div>
    
<?php $this->load->view('include/footer');?>

  <script src="<?= base_url();?>assets/js/jquery.min.js"></script>
  <script src="<?= base_url();?>assets/js/bootstrap.min.js"></script>
  <script type="text/javascript">
  $("#op").click(function() {
    if($('#operator').val()=="and"){
      $('#op').html("Or");
      $('#operator').val("or");
    }  
    else{
      $('#op').html("And");
      $('#operator').val("and");
    }  
  });
  $("#addDir").click(function() {
    if($('#addDirector').val()=="addDir"){
      $('#addDir').html("Hide director");
      $('#addDirector').val("hideDir");
      $('#director').val("");
      $('#director').show();
    }  
    else{
      $('#addDir').html("Add director");
      $('#addDirector').val("addDir");
      $('#director').hide();
    }  
  });
  $("#addY").click(function() {
    if($('#addYear').val()=="addY"){
      $('#addY').html("Hide year");
      $('#addYear').val("hideY");
      $('#year').show();
    }  
    else{
      $('#addY').html("Add year");
      $('#addYear').val("addY");
      $('#year').hide();
      $('#year').val("");
    }  
  });
  $("#addG").click(function() {
    if($('#addGenre').val()=="addG"){
      $('#addG').html("Hide genre");
      $('#addGenre').val("hideG");
      $('#genre').show();
    }  
    else{
      $('#addG').html("Add genre");
      $('#addGenre').val("addG");
      $('#genre').hide();
      $('#genre').val("");
    }  
  });
  </script>
  
  <script type="text/javascript">
  function showBox(movieOrder) {
      var loginBox = $('#login-box');
      $(loginBox).fadeIn(300);

      movie=movies[movieOrder];
      
      $('span#description').html(movie.overview);
      $('span#movie-director').html(movie.director);
      $('span#release').html(movie.release);
      var poster='pulp-fiction';
      if(movie.poster!=null)
       poster=movie.poster; 
      $('img#poster').attr('src', '<?=  base_url()?>assets/img/movies/'+poster+'.jpg');
      
      var popMargTop = ($(loginBox).height() + 24) / 2; 
      var popMargLeft = ($(loginBox).width() + 24) / 2; 

      $(loginBox).css({ 
          'margin-top' : -popMargTop,
          'margin-left' : -popMargLeft
      });

      // Add the mask to body
      $('body').append('<div id="mask"></div>');
      $('#mask').fadeIn(300);

      return false;
  }

  // When clicking on the button close or the mask layer the popup closed
  $('a.close, #mask').live('click', function() { 
    $('#mask , .login-popup').fadeOut(300 , function() {
      $('#mask').remove(); 
    $('img#poster').attr('src', '');  
  }); 
  return false;
  });
 </script>

  