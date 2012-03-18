<?php

use Behat\Behat\Context\ClosuredContextInterface,
    Behat\Behat\Context\TranslatedContextInterface,
    Behat\Behat\Context\BehatContext,
    Behat\Behat\Exception\PendingException;
use Behat\Gherkin\Node\PyStringNode,
    Behat\Gherkin\Node\TableNode;

require dirname(__FILE__) . 
    DIRECTORY_SEPARATOR . '..' . 
    DIRECTORY_SEPARATOR . '..' . 
    DIRECTORY_SEPARATOR . 'setup.php';

/**
 * Features context.
 */
class FeatureContext extends BehatContext
{
    /**
     * Initializes context.
     * Every scenario gets it's own context object.
     *
     * @param   array   $parameters     context parameters (set them up through behat.yml)
     */
    public function __construct(array $parameters)
    {
        // Initialize your context here
        
        $this->useContext('StormSerialiser', new StormSerialiser($parameters));
        $this->useContext('StormComponent', new StormComponent($parameters));
        $this->useContext('StormBolt', new StormBolt($parameters));
    }

}


class MockShellInput 
    extends Storm_Shell_Input_Abstract 
{
    
    private $index = 0;
    private $data = array(
        '/tmp',
        '{}',
        'end',
        '{}',
        'end'
    ); 
    
    public function __construct(Storm_Serializer $serialiser, array $tuple = array()) {
        parent::__construct($serialiser);
        
        if (!empty($tuple) && is_array($tuple)) {
            $this->data[] = $serialiser->encode($tuple);
            $this->data[] = 'end';
        }
    }
    
    public function setConfig($config) {
        $this->data[1] = $config;
    }
    
    public function setContext($context)
    {
        $this->data[3] = $context;
    }
    
    public function read()
    {
        $output = isset($this->data[$this->index])
            ? $this->data[$this->index]
            : '';
        
        $this->index++;
        
        return $output;
    }
    
}

class MockShellOutput 
    extends Storm_Shell_Output_Abstract 
{
    
    private $index = 0;
    private $output = array(); 
    
    public function __construct(Storm_Serializer $serialiser) {
        parent::__construct($serialiser);
    }
    
    protected function output($message)
    {
        $this->output[] = $message;
    }
    
    public function pop()
    {
        $item = null;
        if (isset($this->output[$this->index])) {
            $item = $this->output[$this->index];
            $this->index++;
        }
        
        return $item;
    }
    
}