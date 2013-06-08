# PhpGDC

This is PHP client library to access GoodData REST API.

The most important use case is to upload data into GoodData plaform directly from your web pages.

## Requirements

* PHP5 with zip files support
* Library Httpfull which is downloadable from https://github.com/nategood/httpful
* Admin access to some GoodData project

## Installation

Copy the file gdc.class.php to your web server as well as the Httpful library and set require_once calling accordingly.

## Example

You may use the GDC class to upload data into a dataset. Just write the following:

```php
require_once( 'phpgdc/gdc.class.php' );
/* Some other code */
$gdc = new GDC();
$gdc->login( 'your-username@example.com', 'password' );
$gdc->set_project( 'project-identifier-hash' );
$ds = $gdc->get_dataset( 'dataset-identifier' );
$ds->read_sli_template();
/* Put your data here. Data are given in two-dimensional array. Each inner array represents
a data row, and should have the same number of elements (use $ds->get_num_columns_sli() to 
learn how many). */
$ds->prepare_load( array( array( ... ), ... ) );
$ds->do_etl();
```
