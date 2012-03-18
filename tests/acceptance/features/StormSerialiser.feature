Feature: Storm Serialiser
  In order to use PHP as an implementation of a Storm topology
  As a PHP developer
  I want to be able to encode and decode messages between the shell and its client

Scenario: Encode Emit Message in 32-bit JSON
    Given I use a 32bit JSON serialiser
    When I encode an emit message with tuple "['559','23']"
    Then I expect the result to match the JSON string "{'command':'emit','anchors':[],'tuple':['559','23']}"

Scenario: Encode Ack Message in 32-bit JSON
    Given I use a 32bit JSON serialiser
    When I encode a "ack" message with ID "12345"
    Then I expect the result to match the JSON string "{'command':'ack','id':12345}"

Scenario: Encode Fail Message in 32-bit JSON
    Given I use a 32bit JSON serialiser
    When I encode a "fail" message with ID "12345"
    Then I expect the result to match the JSON string "{'command':'fail','id':12345}"

Scenario: Encode Sync Message in 32-bit JSON
    Given I use a 32bit JSON serialiser
    When I encode a "sync" message
    Then I expect the result to match the JSON string "'sync'"

Scenario: Encode Message with Long Integer ID in 32-bit JSON
    Given I use a 32bit JSON serialiser
    When I encode a "ack" message with ID "-5984936268050827312"
    Then I expect the result to match the JSON string "{'command':'ack','id':-5984936268050827312}"

Scenario: Decode Message in 32-bit JSON
    Given I use a 32bit JSON serialiser
    When I decode the message "{'id':-7116111357102619477,'tuple':['559','23']}"
    Then I expect the ID to maintain its "19" digit precision
      And I expect the tuple to be equal to "['559','23']"