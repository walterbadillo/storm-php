<?php
use Behat\Behat\Context\BehatContext;
use Behat\Behat\Event\ScenarioEvent;
    
require dirname(__FILE__) . 
    DIRECTORY_SEPARATOR . '..' . 
    DIRECTORY_SEPARATOR . '..' . 
    DIRECTORY_SEPARATOR . 'setup.php';

class MockShellBolt extends Storm_Shell_Bolt {
 
    public $tuple = null;
    
    public function process(Storm_Tuple $tuple) {
        $this->runnable = false;
        $this->tuple = $tuple;
    }
    
}

class MockBasicBoltWithSuccess extends Storm_Basic_Bolt {
 
    public $tuple = null;
    
    public function process(Storm_Tuple $tuple) {
        $this->runnable = false;
        $this->tuple = $tuple;
        return true;
    }
    
}

class MockBasicBoltWithFailure extends Storm_Basic_Bolt {
 
    public $tuple = null;
    
    public function process(Storm_Tuple $tuple) {
        $this->runnable = false;
        $this->tuple = $tuple;
        return false;
    }
    
}

class MockBasicBoltWithException extends Storm_Basic_Bolt {
 
    public $tuple = null;
    
    public function process(Storm_Tuple $tuple) {
        $this->runnable = false;
        $this->tuple = $tuple;
        throw new Exception('Test exception');
        return false;
    }
    
}

class StormBolt extends BehatContext {

    private $bolt = null;
    
    private $input = null;
    private $output = null;
    
    private $tuple = array();
    
    public function __construct(array $parameters)
    {
    }

    /**
     * @Given /^a tuple is waiting in the pipeline$/
     */
    public function aTupleIsWaitingInThePipeline()
    {
        $this->tuple = array();
    }

    /**
     * @Given /^the tuple has an ID of "([^"]*)"$/
     */
    public function theTupleHasAnIdOf($id)
    {
        $this->tuple['id'] = $id;
    }

    /**
     * @Given /^the tuple has a value of "([^"]*)"$/
     */
    public function theTupleHasAValueOf($value)
    {
        $this->tuple['tuple'] = json_decode($value, true);
    }

    /**
     * @Given /^I execute the "([^"]*)" bolt$/
     */
    public function iExecuteTheBolt($boltName)
    {
        $serialiser = new Storm_Serializer_32BitJson();
        
        $this->input = new MockShellInput($serialiser, $this->tuple);
        $this->output = new MockShellOutput($serialiser);
        
        $component = new Storm_Component_Shell($this->input, $this->output);
     
        $this->bolt = new $boltName($component);
        
        $this->bolt->run();
        
        $this->output->pop();
    }
    
    /**
     * @When /^the bolt processes the tuple "([^"]*)"$/
     */
    public function theBoltProcessesTheTuple($tupleId)
    {
        if (!is_object($this->bolt->tuple)) {
            throw new Exception('A tuple was not received.');
        }
        
        if (!($this->bolt->tuple instanceof Storm_Tuple)) {
            throw new Exception('The actual tuple is not of class [Storm_Tuple]; instead, found an object of class [' . get_class($this->bolt->tuple) . '].');
        }
        
        $expectedId = $this->tuple['id'];
        $actualId = $this->bolt->tuple->getId();
        if (!$actualId == $expectedId) {
            throw new Exception('The tuple [' . $actualId . '] received does not match expected ID [' . $expectedId . '].');
        }
    }

    /**
     * @Then /^I expect to send a sync command$/
     */
    public function iExpectToSendASyncCommand()
    {
        
        $actual = $this->output->pop();
        
        if ($actual != 'sync') {
            throw new Exception('The sync command was not recognised; instead, found the message [' . $actual . '].');
        }
    }
    
    /**
     * @When /^the bolt succeeds in processing the tuple "([^"]*)"$/
     */
    public function theBoltSucceedsInProcessingTheTuple($tupleId)
    {
        if (!is_object($this->bolt->tuple)) {
            throw new Exception('A tuple was not received.');
        }
        
        if (!($this->bolt->tuple instanceof Storm_Tuple)) {
            throw new Exception('The actual tuple is not of class [Storm_Tuple]; instead, found an object of class [' . get_class($this->bolt->tuple) . '].');
        }
        
        $expectedId = $this->tuple['id'];
        $actualId = $this->bolt->tuple->getId();
        if (!$actualId == $expectedId) {
            throw new Exception('The tuple [' . $actualId . '] received does not match expected ID [' . $expectedId . '].');
        }
    }
    
    /**
     * @When /^the bolt fails to process the tuple "([^"]*)"$/
     */
    public function theBoltFailsToProcessTheTuple($tupleId)
    {
        if (!is_object($this->bolt->tuple)) {
            throw new Exception('A tuple was not received.');
        }
        
        if (!($this->bolt->tuple instanceof Storm_Tuple)) {
            throw new Exception('The actual tuple is not of class [Storm_Tuple]; instead, found an object of class [' . get_class($this->bolt->tuple) . '].');
        }
        
        $expectedId = $this->tuple['id'];
        $actualId = $this->bolt->tuple->getId();
        if (!$actualId == $expectedId) {
            throw new Exception('The tuple [' . $actualId . '] received does not match expected ID [' . $expectedId . '].');
        }
    }

    /**
     * @Then /^I expect to send a ack command$/
     */
    public function iExpectToSendAAckCommand()
    {
        
        $actual = $this->output->pop();
        
        $expected = '{"command":"ack","id":' . $this->tuple['id'] . '}';
        
        if ($actual != $expected) {
            throw new Exception('The ack command was not recognised; instead, found the message [' . $actual . '].');
        }
        
        $actual = $this->output->pop();
        $expected = 'end';
        
        if ($actual != $expected) {
            throw new Exception('The ack command did not end correctly; instead, found the marker [' . $actual . '].');
        }
    }
    
    /**
     * @Then /^I expect to send a fail command$/
     */
    public function iExpectToSendAFailCommand()
    {
        
        $actual = $this->output->pop();
        
        $expected = '{"command":"fail","id":' . $this->tuple['id'] . '}';
        
        if ($actual != $expected) {
            throw new Exception('The fail command was not recognised; instead, found the message [' . $actual . '].');
        }
        
        $actual = $this->output->pop();
        $expected = 'end';
        
        if ($actual != $expected) {
            throw new Exception('The fail command did not end correctly; instead, found the marker [' . $actual . '].');
        }
        
    }
    
    /**
     * @When /^the bolt triggers an exception while processing the tuple "([^"]*)"$/
     */
    public function theBoltTriggersAnExceptionWhileProcessingTheTuple($tupleId)
    {
        if (!is_object($this->bolt->tuple)) {
            throw new Exception('A tuple was not received.');
        }
        
        if (!($this->bolt->tuple instanceof Storm_Tuple)) {
            throw new Exception('The actual tuple is not of class [Storm_Tuple]; instead, found an object of class [' . get_class($this->bolt->tuple) . '].');
        }
        
        $expectedId = $this->tuple['id'];
        $actualId = $this->bolt->tuple->getId();
        if (!$actualId == $expectedId) {
            throw new Exception('The tuple [' . $actualId . '] received does not match expected ID [' . $expectedId . '].');
        }
    }
    
}