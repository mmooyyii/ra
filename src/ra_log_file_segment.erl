-module(ra_log_file_segment).

-export([open/1,
         open/2,
         append/4,
         sync/1,
         read/3,
         close/1,
         max_count/1,
         filename/1]).
% , append/4, read/3, from_ets/2, close/1]).

-include("ra.hrl").

-define(VERSION, 1).

-define(HEADER_SIZE, (16 div 8) + (16 div 8)). % {
-define(DEFAULT_INDEX_MAX_COUNT, 4096).

-define(INDEX_RECORD_SIZE, ((2 * 64 + 3 * 32) div 8)).

-type index_record_data() :: {Term :: ra_term(), % 64 bit
                              Offset :: non_neg_integer(), % 32 bit
                              Length :: non_neg_integer(), % 32 bit
                              Checksum :: integer()}. % CRC32 - 32 bit

-type ra_segment_index() :: #{ra_index() => index_record_data()}.

-record(state,
        {version :: non_neg_integer(),
         max_count = ?DEFAULT_INDEX_MAX_COUNT :: non_neg_integer(),
         filename :: file:filename_all(),
         fd :: maybe(file:io_device()),
         index_size :: pos_integer(),
         index_offset :: pos_integer(),
         data_start :: pos_integer(),
         data_offset :: pos_integer(),
         mode = append :: read | append,
         index = undefined :: maybe(ra_segment_index())
        }).

-type ra_log_file_segment_options() :: #{max_count => non_neg_integer(),
                                         mode => append | read}.
-type state() :: #state{}.

-export_type([state/0]).

-spec open(Filename :: file:filename_all()) ->
    {ok, state()} | {error, term()}.
open(Filename) ->
    open(Filename, #{}).

-spec open(Filename :: file:filename_all(),
           Options :: ra_log_file_segment_options()) ->
    {ok, state()} | {error, term()}.
open(Filename, Options) ->
    AbsFilename = filename:absname(Filename),
    FileExists = filelib:is_file(AbsFilename),
    Mode = maps:get(mode, Options, append),
    {ok, Fd} = case Mode of
                   append ->
                       file:open(AbsFilename, [read, write, raw, binary]);
                   read ->
                       file:open(AbsFilename, [read, raw, read_ahead, binary])
               end,
    case FileExists of
        true ->
            % it is a new file
            % READ and validate VERSION
            MaxCount = read_header(Fd),
            IndexSize = MaxCount * ?INDEX_RECORD_SIZE,
            {NumIndexRecords, DataOffset, Index} = recover_index(Fd, MaxCount),
            {ok, #state{version = 1, max_count = MaxCount,
                        filename = Filename, fd = Fd, index_size = IndexSize,
                        mode = Mode,
                        data_start = ?HEADER_SIZE + IndexSize,
                        data_offset = DataOffset,
                        index_offset = ?HEADER_SIZE + NumIndexRecords * ?INDEX_RECORD_SIZE,
                        % TODO: we don't need an index in memory in append mode
                        index = Index}};
        false ->
            MaxCount = maps:get(max_count, Options, ?DEFAULT_INDEX_MAX_COUNT),
            IndexSize = MaxCount * ?INDEX_RECORD_SIZE,
            ok = write_header(MaxCount, Fd),
            {ok, #state{version = 1,
                        max_count = MaxCount,
                        filename = Filename,
                        fd = Fd,
                        index_size = IndexSize,
                        index_offset = ?HEADER_SIZE,
                        mode = Mode,
                        data_start = ?HEADER_SIZE + IndexSize,
                        data_offset = ?HEADER_SIZE + IndexSize}}
    end.

-spec append(state(), ra_index(), ra_term(), binary()) ->
    {ok, state()} | {error, segment_full}.
append(#state{fd = Fd, index_offset = IndexOffset,
              data_start = DataStart,
              data_offset = DataOffset,
              mode = append} = State,
       Index, Term, Data) ->
    % check if file is full
    case IndexOffset < DataStart of
        true ->
            Length = erlang:byte_size(Data),
            % TODO: check length is less than #FFFFFFFF ??
            Checksum = erlang:crc32(Data),
            IndexData = <<Index:64/integer, Term:64/integer,
                          DataOffset:32/integer, Length:32/integer,
                          Checksum:32/integer>>,
            ok = file:pwrite(Fd, [{DataOffset, Data}, {IndexOffset, IndexData}]),
            % fsync is done explicitly
            {ok, State#state{index_offset = IndexOffset + ?INDEX_RECORD_SIZE,
                             data_offset = DataOffset + Length}};
        false ->
            {error, full}
     end.

-spec sync(state()) -> ok.
sync(#state{fd = Fd}) ->
    ok = file:sync(Fd),
    ok.

-spec read(state(), Idx :: ra_index(), Num :: non_neg_integer()) ->
    [{ra_index(), ra_term(), binary()}].
read(#state{fd = Fd, mode = read, index = Index}, Idx0, Num) ->
    % TODO: should we better indicate when records aren't found?
    % This depends on the semantics we want from a segment
    {Locs, IdxTermCrcs} =
        lists:foldl(fun (Idx, {Ls, ITs} = Acc) ->
                            case Index of
                                #{Idx := {Term, Offset, Length, Crc}} ->
                                    {[{Offset, Length} | Ls],
                                     [{Idx, Term, Crc} | ITs]};
                                _ ->
                                    Acc
                            end
                    end, {[], []}, lists:seq(Idx0 + Num - 1, Idx0, -1)),
    {ok, Datas} = file:pread(Fd, Locs),

    lists:zipwith(fun(TermIdx = {_, _, Crc}, Data) ->
                          Crc = erlang:crc32(Data), % checksum assertion
                          erlang:setelement(3, TermIdx, Data)
                  end,  IdxTermCrcs, Datas).


-spec max_count(state()) -> non_neg_integer().
max_count(#state{max_count = Max}) ->
    Max.

-spec filename(state()) -> file:filename().
filename(#state{filename = Fn}) ->
    filename:absname(Fn).

-spec close(state()) ->
    ok.
close(#state{fd = Fd}) ->
    _ = file:close(Fd),
    ok.

%%% Internal

recover_index(Fd, MaxCount) ->
    IndexSize = MaxCount * ?INDEX_RECORD_SIZE,
    {ok, ?HEADER_SIZE} = file:position(Fd, ?HEADER_SIZE),
    DataOffset = ?HEADER_SIZE + IndexSize,
    case file:read(Fd, IndexSize) of
        {ok, Data} ->
            parse_index_data(Data, DataOffset);
        eof ->
            % if no entries have been written the file hasn't "stretched"
            % to where the data offset starts.
            {0, DataOffset, #{}}
    end.

parse_index_data(Data, DataOffset) ->
    parse_index_data(Data, 0, 0, DataOffset, #{}).

parse_index_data(<<>>, Num, _LastIdx, DataOffset, Index) ->
    % end of data
    {Num, DataOffset, Index};
parse_index_data(<<0:64/integer, 0:64/integer, 0:32/integer,
                   0:32/integer, 0:32/integer, _Rest/binary>>,
                 Num, _LastIdx, DataOffset, Index) ->
    % partially written index
    % end of written data
    {Num, DataOffset, Index};
parse_index_data(<<Idx:64/integer, Term:64/integer,
                   Offset:32/integer, Length:32/integer,
                   Crc:32/integer, Rest/binary>>,
                 Num, LastIdx, _DataOffset, Index0) ->
    % trim index entries if Idx goes "backwards"
    Index = case Idx < LastIdx of
                true -> maps:filter(fun (K, _) when K > Idx -> false;
                                        (_,_) -> true
                                    end, Index0);
                false -> Index0
            end,

    parse_index_data(Rest, Num+1, Idx, Offset + Length,
                     Index#{Idx => {Term, Offset, Length, Crc}}).

write_header(MaxCount, Fd) ->
    Header = <<?VERSION:16/integer, MaxCount:16/integer>>,
    {ok, 0} = file:position(Fd, 0),
    ok = file:write(Fd, Header).

read_header(Fd) ->
    {ok, 0} = file:position(Fd, 0),
    {ok, Buffer} = file:read(Fd, ?HEADER_SIZE),
    case Buffer of
        <<1:16/integer, MaxCount:16/integer>> ->
            MaxCount;
        _ ->
            exit(invalid_segment_version)
    end.