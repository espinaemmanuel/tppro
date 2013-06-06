<?php if ( ! defined('BASEPATH')) exit('No direct script access allowed');
/* Controlador
 *
 * Controlador para el ejemplo de uploadify en Codeigniter
 *
 *
 * @author Oscar Andrés Parra Bolivar [Carxl]
 * @author http://www.programandoweb.com
 * @version 1.0*
 */
class Cualquiera extends CI_Controller
{
	private $_nombreControlador;

	public function __construct()
	{
		parent::__construct();
		
		//crear el nombre del controlador
		//es algo genérico para usarlo en cualquier momento
		$this->_nombreControlador = __CLASS__;
		$this->_nombreControlador = strtolower($this->_nombreControlador);
		
		//cargar el helper url
		$this->load->helper('url');
		
		//cargar el directorio principal
		$this->config->load('sitio');
	}

	public function index()
	{
		//ruta absoluta para subir las imágenes
		$datos['rutaAbsolutaSubir'] = "/{$this->config->item('dirPrincipal')}{$this->_nombreControlador}/upload";
	
		//cargar la vista
		$this->load->view('template', $datos);
	}
	
	public function upload()
	{
		//crear la ruta absoluta
        $targetPath = "{$_SERVER['DOCUMENT_ROOT']}/{$this->config->item('dirPrincipal')}/uploads/";
        
        if (!empty($_FILES)) {
            $nombreArchivo = $_FILES['Filedata']['name'];
			$tempFile = $_FILES['Filedata']['tmp_name'];
			$targetFile =  $targetPath.$nombreArchivo;
			if(move_uploaded_file($tempFile,$targetFile))
			{

			}
		}
		echo 1;
	}
}
/* End of file Cualquiera.php */
/* Location: ./application/controllers/Cualquiera.php */