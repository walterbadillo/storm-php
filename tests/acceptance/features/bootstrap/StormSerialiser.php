<?php
use Behat\Behat\Context\BehatContext;
use Behat\Behat\Event\ScenarioEvent;
    
require dirname(__FILE__) . 
    DIRECTORY_SEPARATOR . '..' . 
    DIRECTORY_SEPARATOR . '..' . 
    DIRECTORY_SEPARATOR . 'setup.php';

class StormSerialiser extends BehatContext
{
    
    private $serialiser = null;
    
    private $testMessageStructures = array(
        'emit' => array(
            'command' => 'emit',
            'anchors' => array(),
            'tuple' => array()
        ),
        'ack' => array(
            'command' => 'ack',
            'id' => null
        ),
        'fail' => array(
            'command' => 'fail',
            'id' => null
        ),
        'sync' => 'sync'
    );
    
    private $encodedMessage = null;
    private $decodedMessage = null;
    
    public function __construct(array $parameters)
    {
    }

    /**
     * @Given /^I use a 32bit JSON serialiser$/
     */
    public function iUseA32BitJsonSerialiser()
    {
        $this->serialiser = new Storm_Serializer_32BitJson();
    }
    
    /**
     * @When /^I encode a "([^"]*)" message$/
     */
    public function iEncodeAMessage($messageType)
    {
        if (!isset($this->testMessageStructures[$messageType])) {
            throw new Exception('Message structure for [' . $messageType . '] is not defined.');    
        }
        
        $message = $this->testMessageStructures[$messageType];
        
        $this->encodedMessage = $this->serialiser->encode($message);
    }
    
    /**
     * @When /^I encode a "([^"]*)" message with ID "([^"]*)"$/
     */
    public function iEncodeAMessageWithId($messageType, $id)
    {
        if (!isset($this->testMessageStructures[$messageType])) {
            throw new Exception('Message structure for [' . $messageType . '] is not defined.');    
        }
        
        $message = $this->testMessageStructures[$messageType];
        
        if (is_array($message) && !empty($id)) {
            $message['id'] = (string) $id;
        }
        
        $this->encodedMessage = $this->serialiser->encode($message);
    }
    
    /**
     * @When /^I encode an emit message with tuple "([^"]*)"$/
     */
    public function iEncodeAnEmitMessageWithTuple($tuple)
    {
        $tuple = str_replace("'", '"', $tuple);
        $tuple = json_decode($tuple, true);
        
        $message = $this->testMessageStructures['emit'];
        $message['tuple'] = $tuple;
        
        $this->encodedMessage = $this->serialiser->encode($message);

    }

    /**
     * @Then /^I expect the result to match the JSON string "([^"]*)"$/
     */
    public function iExpectTheResultToMatchTheJsonString($result)
    {
        
        $result = str_replace("'", '"', $result);
        
        if ($this->encodedMessage != $result) {
            throw new Exception('Actual encoded message [' . $this->encodedMessage . '] does not meet expectation [' . $result . '].');
        }
    }

    /**
     * @When /^I decode the message "([^"]*)"$/
     */
    public function iDecodeTheMessage($message)
    {
        $message = str_replace("'", '"', $message);
        $this->decodedMessage = $this->serialiser->decode($message);
    }

    /**
     * @Then /^I expect the result to be a "([^"]*)" command with ID "([^"]*)"$/
     */
    public function iExpectTheResultToBeACommandWithId($messageType, $id)
    {
        
        if (!is_array($this->decodedMessage)) {
            throw new Exception('The decoded message is expected to be an array.');
        }
        
        if (!isset($this->decodedMessage['command'])) {
            throw new Exception('The decoded message does not have a "command" element."');
        }
        
        if ($this->decodedMessage['command'] != $messageType) {
            throw new Exception('The decoded message does not match ['. $messageType . ']; instead, the message has the command [' . $this->decodedMessage['command'] . '].');
        }
        
        if (!empty($id)) {
            if (!isset($this->decodedMessage['id'])) {
                throw new Exception('The decoded message does not have a "id" element."');
            }

            if ($this->decodedMessage['id'] != $id) {
                throw new Exception('The decoded message ID does not match ['. $id . '].');
            }
        }
    }

    /**
     * @Then /^I expect the ID to maintain its "([^"]*)" digit precision$/
     */
    public function iExpectTheIdToMaintainItsDigitPrecision($precision)
    {
        if (!is_array($this->decodedMessage)) {
            throw new Exception('The decoded message is expected to be an array.');
        }
        
        if (!isset($this->decodedMessage['id'])) {
            throw new Exception('The decoded message does not have a "id" element."');
        }
        
        $id = str_replace('-', '', $this->decodedMessage['id']);
        
        if (strlen($id) != $precision) {
            throw new Exception('The decoded message ID ['. $this->decodedMessage['id'] . '] does not have [' . $precision . '] digits.');
        }
    }
    
    /**
     * @Then /^I expect the tuple to be equal to "([^"]*)"$/
     */
    public function iExpectTheTupleToBeEqualTo($tuple)
    {
        if (!is_array($this->decodedMessage)) {
            throw new Exception('The decoded message is expected to be an array.');
        }
        
        if (!isset($this->decodedMessage['tuple'])) {
            throw new Exception('The decoded message does not have a "tuple" element."');
        }
        
        $tuple = str_replace("'", '"', $tuple);
        $tuple = json_decode($tuple, true);
        
        if ($this->decodedMessage['tuple'] != $tuple) {
            throw new Exception('The decoded tuple does not match its expectation.');
        }
    }
    
}
