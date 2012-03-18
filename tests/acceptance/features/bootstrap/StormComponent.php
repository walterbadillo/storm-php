<?php
use Behat\Behat\Context\BehatContext;
use Behat\Behat\Event\ScenarioEvent;
    
require dirname(__FILE__) . 
    DIRECTORY_SEPARATOR . '..' . 
    DIRECTORY_SEPARATOR . '..' . 
    DIRECTORY_SEPARATOR . 'setup.php';



class StormComponent extends BehatContext {

    private $component = null;
    
    private $input = null;
    private $output = null;
    
    private $stormConfig = null;
    private $topologyContext = null;
    
    public function __construct(array $parameters)
    {
    }
    
    /**
     * @Given /^a Storm configuration of "([^"]*)"$/
     */
    public function aStormConfigurationOf($config)
    {
        $this->stormConfig = $config;
    }

    /**
     * @Given /^a topology context of "([^"]*)"$/
     */
    public function aTopologyContextOf($context)
    {
        $this->topologyContext = $context;
    }

    /**
     * @Given /^I create the "([^"]*)" component$/
     */
    public function iDefineTheComponent($componentClass)
    {
        $serialiser = new Storm_Serializer_32BitJson();
        
        $this->input = new MockShellInput($serialiser);
        $this->input->setConfig($this->stormConfig);
        $this->input->setContext($this->topologyContext);
        
        $this->output = new MockShellOutput($serialiser);
        
        $this->component = new $componentClass($this->input, $this->output);
    }

    /**
     * @When /^I initialise the component$/
     */
    public function iInitialiseTheComponent()
    {
        $this->component->initialise();
    }

    /**
     * @Then /^I expect to send the process ID$/
     */
    public function iExpectToSendTheProcessId()
    {
        $actualPid = $this->output->pop();
        
        $expectedPid = getmypid();
        
        if ($expectedPid != $actualPid) {
            throw new Exception('The actual process ID [' . $actualPid .'] does not match the expected ID [' . $expectedPid . '].');
        }

    }

    /**
     * @Given /^I expect to receive the Storm configuration$/
     */
    public function iExpectToReceiveTheStormConfiguration()
    {
        $actualConfig = $this->component->getConfiguration();
        
        $expectedConfig = $this->stormConfig;
        
        if ($expectedConfig != $actualConfig) {
            throw new Exception('The actual configuration [ ' . $actualConfig .' ] does not match the expected configuration [ ' . $expectedConfig . ' ].');
        }
    }
    
    /**
     * @Given /^I expect to receive the topology context$/
     */
    public function iExpectToReceiveTheTopologyContext()
    {
        $actualContext = $this->component->getTopologyContext();
        
        $expectedContext = $this->topologyContext;
        
        if ($expectedContext != $actualContext) {
            throw new Exception('The actual context [ ' . $actualContext .' ] does not match the expected context [ ' . $expectedContext . ' ].');
        }
    }
   

}