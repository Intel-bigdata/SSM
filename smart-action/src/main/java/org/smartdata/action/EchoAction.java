package org.smartdata.action;

import org.smartdata.action.annotation.ActionSignature;

@ActionSignature(
        actionId = "echo",
        displayName = "echo",
        usage = EchoAction.PRINT_MESSAGE + " $message"
)
public class EchoAction extends SmartAction {
    public static final String PRINT_MESSAGE = "-msg";

    @Override
    protected void execute() throws Exception {
        this.appendResult(getArguments().get(PRINT_MESSAGE));
        // System.out.println(getArguments().get(PRINT_MESSAGE));
    }
}
