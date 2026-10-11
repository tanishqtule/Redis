package redis.command;

/** Thrown by a handler to send an error reply. The message is sent as-is, so include the code ("ERR ..."). */
public class CommandException extends RuntimeException {
    public CommandException(String message) {
        super(message, null, false, false);
    }

    public static CommandException syntax() {
        return new CommandException("ERR syntax error");
    }

    public static CommandException notInteger() {
        return new CommandException("ERR value is not an integer or out of range");
    }
}
