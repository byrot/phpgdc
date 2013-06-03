<?php

/* 
 *   gdc.class.php
 *
 *   GoodData PHP Client Library
 *
 *   Copyright (C) 2013   Tomas Jirotka <tomjir@gmail.com>
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation, either version 3 of the License, or
 *   (at your option) any later version.
 *   
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *   
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

require_once( 'httpful/bootstrap.php' );

class GDC {

	const GDC_REST_SERVER   = 'https://secure.gooddata.com';
	const GDC_FTP_SERVER    = 'secure-di.gooddata.com';
	const GDC_COOKIE_AUTH   = 'GDCAuthSST';
	const GDC_COOKIE_TOKEN  = 'GDCAuthTT';
	const GDC_API_LOGIN     = '/gdc/account/login';
	const GDC_API_TOKEN     = '/gdc/account/token';
	const GDC_API_MD        = '/gdc/md';
	const GDC_API_ETL       = '/gdc/md/<project>/etl/pull';
	const GDC_API_DATASETS  = '/gdc/md/<project>/data/sets';
	const GDC_API_SLI       = '/gdc/md/<project>/ldm/singleloadinterface';
	const GDC_API_ID_TO_URI = '/gdc/md/<project>/identifiers';

	protected $server;
	protected $cookies;
	protected $project;
	protected $templates;
	protected $debug;
	protected $user;

	public function __construct( $server = self::GDC_REST_SERVER ) {
		$this->server = $server;
		$this->debug = FALSE;
		$this->templates = array();
		$this->user = array();
		$this->cookies = array();
		$this->cookies['auth' ] = '';
		$this->cookies['token'] = '';
	}


#########    GD AUTHENTICATION   #########

	# Authenticate against GDC API, get authentication and security token
	# Params: username string, password string
	# Return: authentication token string
	public function login( $login, $passwd ) {
		$r = $this->_post( self::GDC_API_LOGIN, '{"postUserLogin":{"login":"'.$login.'","password":"'.$passwd.'","remember":1}}' );
		$this->cookies['auth'] = $this->_extract_cookie( $r, self::GDC_COOKIE_AUTH );

		if( $this->cookies['auth'] == '' ) {
			throw new Exception( 'Authentication failed' );
		}
		$this->token();
		$this->user['name'] = $login;
		$this->user['passwd'] = $passwd;

		return $this->cookies['auth'];
	}

	# Refresh security token
	# Params: authentication token string optional
	# Returns: security token string
	public function token( $token = '' ) {
		if( isset( $token ) && $token != '' && $this->cookies['auth'] == '' ) {
			$this->cookies['auth'] = $token;
		}
		$this->cookies['token'] = '';
		$r = $this->_get( self::GDC_API_TOKEN );
		$this->cookies['token'] = $this->_extract_cookie( $r, self::GDC_COOKIE_TOKEN );
		if( $this->cookies['token'] == '' ) {
			throw new Exception( 'Getting token failed' );
		}
		return $this->cookies['token'];
	}


#########    GD PROJECTS    #########

	# GET list of projects enabled with user
	# Return: array ( id => title )
	public function list_projects() {
		$r = $this->_get( self::GDC_API_MD );
		$p = array();
		foreach( $r->body->about->links as $prj ) {
			$p[ $prj->identifier ] = $prj->title;
		}
		return $p;
	}

	# Set working project
	# Param: project identifier string
	# Return: string
	public function set_project( $project ) {
		return $this->project = $project;
	}


#########    GD DATESETS    #########

	# GET list of project dataset
	# Return: array
	public function list_datasets() {
		$r = $this->_get( self::GDC_API_DATASETS );
		$d = array();
		foreach( $r->body->dataSetsInfo->sets as $ds ) {
			$d[ $ds->meta->identifier ] = $ds;
		}
		return $d;
	}

	# Download SLI dataset template
	# Param: dataset identifier string
	# Return: string filename
	public function get_sli_template( $dataset ) {
		$r = $this->_get( self::GDC_API_SLI . "/$dataset/template" );
		$tmp = '/tmp/gdc-' . $this->project . '-template-' . $dataset . '-' . $this->_rnd() . '.zip';

		$fp = fopen( $tmp, 'w' );
		try {
			fwrite( $fp, $r->body );
		} catch( Exception $e ) {
			fclose( $fp );
			return 0;
		}
		fclose( $fp );

		return $tmp;
	}

	# Download and read details from SLI manifest
	# Param: dataset identifier string
	# Return: array( info => upload_manifest, csv => csv_columns, zipfile => filename )
	public function read_sli_template( $dataset ) {
		$zipfile = $this->get_sli_template( $dataset );
		$store = array();
		$store['zipfile'] = $zipfile;

		$zip = zip_open( $zipfile );

		while( $entry = zip_read( $zip ) ) {
			zip_entry_open( $zip, $entry, 'r' );

			$entry_name = zip_entry_name( $entry );
			$entry_content = zip_entry_read( $entry, zip_entry_filesize( $entry ) );

			if( $entry_name == 'upload_info.json' ) {
				$store['info'] = json_decode( $entry_content );
			}

			if( $entry_name == $dataset . '.csv' ) {
				$store['csv'] = array_flip( explode( ',', $entry_content ) );
			}

			zip_entry_close( $entry );
		}

		zip_close( $zip );

		$store = $this->read_sli_manifest( $store );
		$this->templates[$this->project][$dataset] = $store;

		return $store;
	}

	# Get details on CSV columns
	# Param: array( info => upload_info.json, csv => columns )
	# Return: the same structure with modified data
	protected function read_sli_manifest( $store ) {
		foreach( $store['info']->dataSetSLIManifest->parts as $column ) {
			$obj = array_values( get_object_vars( $this->get_object( $column->populates[0] ) ) );
			$store['csv'][$column->columnName] = array(
				'title' => $obj[0]->meta->title,
				'uri' => $obj[0]->meta->uri,
				'category' => $obj[0]->meta->category,
				'identifier' => $obj[0]->meta->identifier
			);
		}
		return $store;
	}

	# Set dataset SLI mode to INCREMENTAL
	# Param: identifier string
	# Return: array - dataset store structure
	public function set_sli_incremental( $dataset ) {
		$store = $this->templates[$this->project][$dataset];

		if( !isset( $store ) ) {
			throw new Exception( 'Call read_sli_template method first' );
		}

		foreach( $store['info']->dataSetSLIManifest->parts as $col ) {
			$col->mode = 'INCREMENTAL';
		}

		$this->templates[$this->project][$dataset] = $store;

		return $store;
	}

	# GET object definition and meta
	# Param: identifier string
	# Return: object
	public function get_object( $identifier ) {
		$r = $this->_post( self::GDC_API_ID_TO_URI, '{"identifierToUri":["'.$identifier.'"]}' );
		return $this->_get( $r->body->identifiers[0]->uri )->body;
	}

	# Get number of columns for dataset SLI load
	# Param: identifier string
	# Return: integer
	public function get_num_columns_sli( $dataset ) {
		$store = $this->templates[$this->project][$dataset];

		if( !isset( $store ) ) {
			throw new Exception( 'Call read_sli_template method first' );
		}

		return count( $store['csv'] );
	}

	# Prepare ZIP file for dataset load
	# Params: identifier string, array of arrays (table)
	# Return: zip archive filename
	public function prepare_load( $dataset, $data ) {
		$store = $this->templates[$this->project][$dataset];

		if( !isset( $store ) ) {
			throw new Exception( 'Call read_sli_template method first' );
		}

		$csv = $this->data_to_csv( $dataset, $data );
		$info = json_encode( $store['info'] );

		$zip = new ZipArchive();
		$fn  = $store['zipfile'] . '.data.zip';

		if( $zip->open( $fn, ZipArchive::CREATE ) !== TRUE ) {
			throw new Exception( 'Cannot open file ' . $fn );
		}

		$zip->addFromString( 'upload_info.json', $info );
		$zip->addFromString( $dataset . '.csv' , $csv  );
		$zip->close();

		$this->debug( "FILE $fn" );

		return $fn;
	}

	# Run ETL task
	# Param: filename of local zip file to upload
	# Return: boolean
	public function do_etl( $filename ) {
		$dir = $this->do_upload( $filename );
		if( $dir ) {
			$this->_post( self::GDC_API_ETL, '{"pullIntegration":"'.$dir.'"}' );
			return TRUE;
		} else {
			return FALSE;
		}
	}

	# Stores data into templated CSV dataset
	# Params: identifier string, array of arrays
	# Return: string csv
	protected function data_to_csv( $dataset, $data ) {
		$store = $this->templates[$this->project][$dataset];
		$numcols = count( $store['csv'] );
		$i = 1; $e = 0;
		$csv = implode( ',', array_keys( $store['csv'] ) );

		foreach( $data as $row ) {
			if( count( $row ) == $numcols ) {
				$csv .= "\n" . implode( ',', $row );
			} else {
				$this->warn( 'Invalid number of records at row ' . $i . ' (' . implode( ',', $row ) . ') - skipping' );
				++$e;
			}
			++$i;
		}
		$this->debug( 'ROWS ' . ( $i - $e - 1 ) . ' added, ' . $e . ' skipped' );

		return $csv;
	}

	# Upload zip to ftp server
	# Param: filename
	# Return: remote directory string
	protected function do_upload( $filename ) {
		try {
			$ftp = ftp_ssl_connect( self::GDC_FTP_SERVER ) or die( 'Ftp server not accessible' );

			ftp_login( $ftp, $this->user['name'], $this->user['passwd'] ) or die( 'Invalid credentials for ftp login' );

			ftp_pasv( $ftp, TRUE );
			$dir = $this->_rnd();
			ftp_mkdir( $ftp, $dir );
			ftp_chdir( $ftp, $dir );
			ftp_put( $ftp, 'upload.zip', $filename, FTP_BINARY ) or die( 'File upload failed' );
			ftp_close( $ftp );
			$this->debug( 'SFTP upload to ' . $dir . '/upload.zip' );
		} catch( Exception $e ) {
			return FALSE;
		}

		return $dir;
	}


#########    HTTP HELPER METHODS    #########

	# Extract cookie from reponse
	# Params: \Httpful\Response, cookie name string
	# Return: string
	protected function _extract_cookie( $response, $name ) {
		$cookie = $response->headers->offsetGet( 'set-cookie' );
		$parts = explode( ';', $cookie );
		# Important is only the first part
		list( $key, $value ) = explode( '=', $parts[0] );
		if( $key == $name ) {
			return $value;
		}
		return '';
	}

	# Send POST request to URI with JSON encoded parameters
	# Params: URI string, JSON data string
	# Returns: \Httpful\Response
	protected function _post( $uri, $json = '' ) {
		$uri = $this->_bind_uri( $uri );
		$this->debug( 'POST ' . $uri . ' <<' . $json . '>>' );

		try {
			$r = \Httpful\Request::post( $this->server . $uri );
			$r = $this->_add_cookie( $r );
			$r->addHeader( 'Accept', 'application/json' );
			return $r->sendsJson()->body( $json )->send();
		} catch( Exception $e ) {
			print_r( $e );
			return 0;
		}
	}

	# GDC GET request
	# Param: URI string
	# Returns: \Httpful\Response
	protected function _get( $uri ) {
		$uri = $this->_bind_uri( $uri );
		$this->debug( 'GET  ' . $uri );

		try {
			$r = \Httpful\Request::get( $this->server . $uri );
			$r = $this->_add_cookie( $r );
			$r->addHeader( 'Accept', 'application/json, application/zip' );
			return $r->send();
		} catch( Exception $e ) {
			print_r( $e );
			return 0;
		}
	}

	# Set GDC authentication cookie
	# Param: \Httpful\Request
	# Return: \Httpful\Request
	protected function _add_cookie( $r ) {
		if( $this->cookies['auth'] != '' ) {
			$r->addHeader( 'Cookie', self::GDC_COOKIE_AUTH . '=' . $this->cookies['auth'] );
			$this->debug( '  AUTH ' . $this->cookies['auth'] );
		}
		if( $this->cookies['token'] != '' ) {
			$r->addHeader( 'Cookie', self::GDC_COOKIE_TOKEN . '=' . $this->cookies['token'] );
			$this->debug( '  TOKN ' . $this->cookies['token'] );
		}
		return $r;
	}

	# Substitute placeholders in URI
	# Param: URI string
	# Return: URI string
	protected function _bind_uri( $uri ) {
		return str_replace( '<project>', $this->project, $uri );
	}


#########    DEBUGGING SERVICE    #########

	# Turn on debugging messages
	public function debug_on() {
		return $this->debug = TRUE;
	}

	# Turn on debugging messages
	public function debug_off() {
		return $this->debug = FALSE;
	}

	# Print debug message
	# Param: string
	protected function debug( $msg ) {
		if( $this->debug ) {
			echo "$msg\n";
			return TRUE;
		} else {
			return FALSE;
		}
	}

	# Print warning message
	# Param: string
	protected function warn( $msg ) {
		echo "WARN [[$msg]]\n";
		return TRUE;
	}


#########    MISCELANEOUS UTILS    #########

	# Generate "random" string of given length
	# Param: integer
	# Return: string
	protected function _rnd( $length = 6 ) {
		$characters = '0123456789abcdefghijklmnopqrstuvwxyz';
		$string = '';
		for( $p = 0; $p < $length; $p++ ) {
			$string .= $characters[ mt_rand( 0, 35 ) ];
		}
		return $string;
	}

}

?>
